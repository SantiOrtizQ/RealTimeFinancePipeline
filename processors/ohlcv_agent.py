import os
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import faust
from sqlalchemy import create_engine, text
import redis as redis_client

from prometheus_client import Counter, Histogram, start_http_server

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

KAFKA_BOOTSTRAP=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TIMESCALE_USER=os.getenv("TIMESCALE_USER", "financeuser")
TIMESCALE_PASS=os.getenv("TIMESCALE_PASSWORD", "financepass")
TIMESCALE_DB=os.getenv("TIMESCALE_DB", "financedb")
TIMESCALE_URL=f"postgresql://{TIMESCALE_USER}:{TIMESCALE_PASS}@localhost:5432/{TIMESCALE_DB}"

app=faust.App(
    "ohlcv-agent",
    broker=f"kafka://{KAFKA_BOOTSTRAP}",
    value_serializer="json"
)

TICKS_PROCESSED=Counter(
    "ohlcv_ticks_processed_total",
    "Total number of ticks processed by the OHLCV agent",
    ["symbol"]
)
BARS_FLUSHED=Counter(
    "ohlcv_bars_flushed_total",
    "total number of OHLCV bars flushed to TimescaleDB",
    ["symbol"]
)
BAR_FLUSH_LATENCY=Histogram(
    "ohlcv_bar_flush_latency_seconds",
    "Latency of flushing a bar to TimescaleDB"
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

class OhlcvBar(faust.Record):
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    window_start: int
    window_end: int


ticks_topic=app.topic("raw.ticks", value_type=TickEvent)
ohlcv_topic=app.topic("processed.ohlcv", value_type=OhlcvBar)

engine=create_engine(TIMESCALE_URL)

redis=redis_client.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIST_PORT", 6379)),
    decode_responses=True
)

def ensure_table():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ohlcv_bars(
                          symbol TEXT NOT NULL,
                          open DOUBLE PRECISION NOT NULL,
                          high DOUBLE PRECISION NOT NULL,
                          low DOUBLE PRECISION NOT NULL,
                          close DOUBLE PRECISION NOT NULL,
                          volume BIGINT NOT NULL,
                          window_start TIMESTAMPTZ NOT NULL,
                          window_end TIMESTAMPTZ NOT NULL
                          );
    """))
        conn.execute(text("""
            SELECT create_hypertable(
                          'ohlcv_bars', 'window_start',
                          if_not_exists=>TRUE
                          );
    """))
        conn.commit()
    logger.info("ohlcv_bars hypertable ready")


def insert_bar(bar: OhlcvBar):
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO ohlcv_bars
                (symbol, open, high, low, close, volume, window_start, window_end)
            VALUES
                (:symbol, :open, :high, :low, :close, :volume,
                          to_timestamp(:ws/1000.0), to_timestamp(:we/1000.0))
        """), {
            "symbol": bar.symbol,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "ws": bar.window_start,
            "we": bar.window_end
        })
        conn.commit()


@app.agent(ticks_topic)
async def process_ticks(ticks):
    async for tick in ticks:
        logger.info(f"Recieved tick: {tick.symbol} @ {tick.price}")


windows={}


@app.timer(interval=60.0)
async def flush_windows():
    if not windows:
        return
    
    now_ms=int(datetime.now(timezone.utc).timestamp()*1000)
    window_size_ms=60_000
    current_window=(now_ms//window_size_ms)*window_size_ms

    to_flush=[
        key for key in windows if key[1]<current_window
    ]

    for key in to_flush:
        data=windows.pop(key)
        symbol, window_start=key
        window_end=window_start+window_size_ms

        bar=OhlcvBar(
            symbol=symbol,
            open=data["open"],
            high=data["high"],
            low=data["low"],
            close=data["close"],
            volume=data["volume"],
            window_start=window_start,
            window_end=window_end
        )

        try:
            with BAR_FLUSH_LATENCY.time():
                insert_bar(bar)
            await ohlcv_topic.send(key=symbol, value=bar)
            logger.info(
                f"Flushed bar: {symbol} | "
                f"O={bar.open} H={bar.high} L={bar.low} C={bar.close} "
                f"V={bar.volume}"
            )
        except Exception as e:
            logger.error(f"Failed to flush bar for {symbol}: {e}")


@app.agent(ticks_topic)
async def aggregate_ticks(ticks):
    async for tick in ticks:
        TICKS_PROCESSED.labels(symbol=tick.symbol).inc()

        window_size_ms=60_000
        window_start=(tick.timestamp//window_size_ms)*window_size_ms
        key=(tick.symbol, window_start)

        if key not in windows:
            windows[key]={
                "open": tick.price,
                "high": tick.price,
                "low": tick.price,
                "close": tick.price,
                "volume": tick.volume
            }
        else:
            w=windows[key]
            w["high"]=max(w["high"], tick.price)
            w["low"]=min(w["low"], tick.price)
            w["close"]=tick.price
            w["volume"] += tick.volume
            redis.set(f"ticker:{tick.symbol}", tick.price, ex=10)


@app.on_started.connect
async def on_started(app, **kwargs):
    start_http_server(6066)
    ensure_table()
    logger.info("OHLCV agent started")


if __name__=="__main__":
    app.main()