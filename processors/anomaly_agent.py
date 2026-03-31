import os
import logging
from collections import deque
from dotenv import load_dotenv
import faust
from sqlalchemy import create_engine, text

from prometheus_client import Counter, start_http_server

load_dotenv()
logging.basicConfig(logging.INFO)
logger=logging.getLogger(__name__)


KAFKA_BOOTSTRAP=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TIMESCALE_USER=os.getenv("TIMESCALE_USER", "financeuser")
TIMESCALE_PASS=os.getenv("TIMESCALE_PASSWORD", "financepass")
TIMESCALE_DB=os.getenv("TIMESCALE_DB", "financedb")
TIMESCALE_URL=f"postgresql://{TIMESCALE_USER}@localhost:5432/{TIMESCALE_DB}"

WINDOW_SIZE=20
Z_SCORE_THRESHOLD=3.0


app=faust.App(
    "anomaly-agent",
    broker=f"kafka://{KAFKA_BOOTSTRAP}",
    value_serializer="json"
)


TICKS_PROCESSED=Counter(
    "anomaly_ticks_processed_total",
    "Total ticks processed by the anomaly agent",
    ["symbol"]
)
ANOMALIES_DETECTED=Counter(
    "anomaly_detections_total",
    "Total anomalies detected",
    ["symbol"]
)

class TickEvent(faust.Record):
    symbol: str
    price: float
    bid: float
    ask: float
    volume: int
    timestamp: int
    source: str
    bid_size: int=None



ticks_topic=app.topic("raw.ticks", value_type=TickEvent)

engine=create_engine(TIMESCALE_URL)
price_windows: dict[str, deque]={}


def ensure_table():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS anomalies (
                          symbol TEXT NOT NULL,
                          price DOUBLE PRECISION NOT NULL,
                          mean DOUBLE PRECISION NOT NULL,
                          std_dev DOUBLE PRECISION NOT NULL,
                          z_score DOUBLE PRECISION NOT NULL,
                          detected_at TIMESTAMPTZ NOT NULL
            );
        """))
        conn.execute(text("""
            SELECT create_hypertable(
                          'anomalies', 'detected_at',
                          if_not_exists=>TRUE
            );
        """))
        conn.commit()
    logger.info("anomalies hypertable ready")


def compute_z_score(prices: deque) -> tuple[float, float, float]:
    n=len(prices)
    mean=sum(prices)/n
    variance=sum((p-mean)**2 for p in prices)/n
    std_dev=variance**0.5

    if std_dev==0:
        return mean, std_dev, 0.0
    
    z_score=(prices[-1]-mean)/std_dev
    return mean, std_dev, z_score


def insert_anomaly(symbol: str, price: float, mean: float, std_dev: float, z_score: float, timestamp_ms: int):
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO anomalies
                (symbol, price, mean, std_dev, z_score, detected_at)
            VALUES
                (:symbol, :price, :mean, :std_dev, :z_score,
                    to_timestamp(:ts/1000.0))
        """), {
            "symbol": symbol,
            "price": price,
            "mean": mean,
            "std_dev": std_dev,
            "z_score": z_score,
            "ts": timestamp_ms
        })
        conn.commit()



@app.agent(ticks_topic)
async def detect_anomalies(ticks):
    async for tick in ticks:
        TICKS_PROCESSED.labels(symbol=tick.symbol).inc()

        symbol=tick.symbol

        if symbol not in price_windows:
            price_windows[symbol]=deque(maxlen=WINDOW_SIZE)
        
        price_windows[symbol].append(tick.price)

        if len(price_windows[symbol])<WINDOW_SIZE:
            logger.debug(
                f"Warming up window for {symbol}"
                f"({len(price_windows[symbol])}/{WINDOW_SIZE})"
            )
            continue

        mean, std_dev, z_score=compute_z_score(price_windows[symbol])

        if abs(z_score)>=Z_SCORE_THRESHOLD:
            logger.warning(
                f"ANOMALY detected: {symbol} | "
                f"price={tick.price:.4f} mean={mean:.4f} "
                f"std={std_dev:.4f} z={z_score:.2f}"
            )
            ANOMALIES_DETECTED.labels(symbol=symbol).inc()
            try:
                insert_anomaly(
                    symbol=symbol,
                    price=tick.price,
                    mean=mean,
                    std_dev=std_dev,
                    z_score=z_score,
                    timestamp_ms=tick.timestamp
                )
            except Exception as e:
                logger.error(f"Failed to insert anomaly for {symbol}: {e}")
        else:
            logger.debug(f"{symbol} z={z_score:.2f} - normal")


@app.on_started.connect
async def on_started(app, **kwargs):
    start_http_server(6067)
    ensure_table()
    logger.info("Anomaly detection agent started")


if __name__=="__main__":
    app.main()
