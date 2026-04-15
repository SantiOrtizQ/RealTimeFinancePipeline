import os
import json
import signal
import logging
from collections import deque
from dotenv import load_dotenv

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from sqlalchemy import create_engine, text
from prometheus_client import Counter, start_http_server

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

KAFKA_BOOTSTRAP=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL=os.getenv("SCHEMA_REGISTRY_URL")

TIMESCALE_USER=os.getenv("TIMESCALE_USER", "financeuser")
TIMESCALE_PASS=os.getenv("TIMESCALE_PASSWORD", "financepass")
TIMESCALE_DB=os.getenv("TIMESCALE_DB", "financedb")
TIMESCALE_URL=f"postgresql://{TIMESCALE_USER}:{TIMESCALE_PASS}@localhost:5432/{TIMESCALE_DB}"

WINDOW_SIZE=20
Z_SCORE_THRESHOLD=3.0

engine=create_engine(TIMESCALE_URL)
price_windows: dict[str, deque]={}
running=True

TICKS_PROCESSED=Counter("anomally_ticks_processed_total", "Ticks processed", ["symbol"])
ANOMALIES_DETECTED=Counter("anomaly_detections_total", "Anomalies found", ["symbol"])



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
        conn.execute(text(
            """
            SELECT create_hypertable(
                'anomalies', 'detected_at',
                if_not_exists => TRUE
            );
            """
        ))
        conn.commit()
    logger.info("anomalies hypertable ready")



def compute_z_score(prices: deque) -> tuple[float, float, float]:
    n=len(prices)
    mean=sum(prices)/n
    std_dev=(sum((p-mean)**2 for p in prices)/n)**0.5
    if std_dev==0:
        return mean, std_dev, 0.0
    return mean, std_dev, (prices[-1]-mean)/std_dev


def insert_anomaly(symbol, price, mean, std_dev, z_score, timestamp_ms):
    with engine.connect() as conn:
        conn.execute(text(
            """
            INSERT INTO anomalies
                (symbol, price, mean, std_dev, z_score, detected_at)
            VALUES
                (:symbol, :price, :mean, :std_dev, :z_score, to_timestamp(:ts/1000.0))
            """
        ), {
            "symbol": symbol,
            "price": price,
            "mean": mean,
            "std_dev": std_dev,
            "z_score": z_score,
            "ts": timestamp_ms
        })
        conn.commit()


def process_tick(msg_value: dict):
    symbol=msg_value.get("symbol")
    price=float(msg_value.get("price", 0))
    timestamp=int(msg_value.get("timestamp", 0))

    if not symbol or not price:
        return
    
    TICKS_PROCESSED.labels(symbol=symbol).inc()
    
    if symbol not in price_windows:
        price_windows[symbol]=deque(maxlen=WINDOW_SIZE)
    price_windows[symbol].append(price)

    if len(price_windows[symbol])<WINDOW_SIZE:
        logger.debug(f"Warming up {symbol} ({len(price_windows[symbol])}/{WINDOW_SIZE})")
        return
    
    mean, std_dev, z_score=compute_z_score(price_windows[symbol])

    if abs(z_score)>=Z_SCORE_THRESHOLD:
        logger.warning(
            f"ANOMALY: {symbol} price={price:.4f} "
            f"mean={mean:.4f} z={z_score:.2f}"
        )
        ANOMALIES_DETECTED.labels(symbol=symbol).inc()
        try:
            insert_anomaly(symbol, price, mean, std_dev, z_score, timestamp)
        except Exception as e:
            logger.error(f"Failed to insert anomaly: {e}")


def run():
    global running
    ensure_table()
    start_http_server(6067)
    logger.info("Anomaly agent started - metrics on port 6067")


    sr_client=SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer=AvroDeserializer(sr_client)

    consumer=DeserializingConsumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "anomaly-agent",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "value.deserializer": avro_deserializer
    })
    consumer.subscribe(["raw.ticks"])

    def shutdown(signum, frame):
        global running
        logger.info("Shutting down anomaly agent...")
        running=False
        consumer.close()
    
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while running:
        msg=consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Consumer error: {msg.error()}")
            continue
        try:
            value=msg.value()
            process_tick(value)
        except Exception as e:
            logger.error(f"Failed to process message: {e}")


if __name__=="__main__":
    run()