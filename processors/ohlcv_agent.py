import os
import time
import logging
import signal
import threading
from datetime import datetime, timezone
from dotenv import load_dotenv

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from sqlalchemy import create_engine, text
import redis as redis_client

from prometheus_client import Counter, Histogram, start_http_server

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

KAFKA_BOOTSTRAP=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
SCHEMA_REGISTRY_URL=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")

TIMESCALE_USER=os.getenv("TIMESCALE_USER", "financeuser")
TIMESCALE_PASS=os.getenv("TIMESCALE_PASSWORD", "financepass")
TIMESCALE_DB=os.getenv("TIMESCALE_DB", "financedb")
TIMESCALE_URL=f"postgresql://{TIMESCALE_USER}:{TIMESCALE_PASS}@localhost:5432/{TIMESCALE_DB}"

WINDOW_SIZE_MS=60_000

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





engine=create_engine(TIMESCALE_URL)

redis=redis_client.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6380)),
    decode_responses=True
)

windows: dict={}
running=True

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
                          if_not_exists => TRUE
                          );
    """))
        conn.commit()
    logger.info("ohlcv_bars hypertable ready")


def insert_bar(symbol, open_, high, low, close, volume, ws, we):
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO ohlcv_bars
                (symbol, open, high, low, close, volume, window_start, window_end)
            VALUES
                (:symbol, :open, :high, :low, :close, :volume,
                          to_timestamp(:ws/1000.0), to_timestamp(:we/1000.0))
        """), {
            "symbol": symbol,
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "ws": ws,
            "we": we
        })
        conn.commit()


def flush_windows(force: bool=False):
    if not windows:
        return
    
    now_ms=int(datetime.now(timezone.utc).timestamp()*1000)
    current_window=(now_ms//WINDOW_SIZE_MS)*WINDOW_SIZE_MS

    to_flush=[
        key for key in windows if key[1]<current_window
    ]

    for key in to_flush:
        data=windows.pop(key)
        symbol, window_start=key
        window_end=window_start+WINDOW_SIZE_MS
        try:
            with BAR_FLUSH_LATENCY.time():
                insert_bar(
                    symbol,
                    data['open'],
                    data['high'],
                    data['low'],
                    data['close'],
                    data['volume'],
                    window_start,
                    window_end
                )
            BARS_FLUSHED.labels(symbol=symbol).inc()
            logger.info(
                f"Flushed bar: {symbol} | "
                f"O={data['open']} H={data['high']} L={data['low']} C={data['close']} "
                f"V={data['volume']}"
            )
        except Exception as e:
            logger.error(f"Failed to flush bar for {symbol}: {e}")

def flush_loop():
    while running:
        time.sleep(60)
        flush_windows()


def process_tick(msg_value: dict):
    symbol=msg_value.get("symbol")
    price=float(msg_value.get("price", 0))
    volume=int(msg_value.get("volume", 2))
    timestamp=int(msg_value.get("timestamp", 0))

    #logger.info(f"process_tick called: {symbol} @ {price}")

    if not symbol or not price:
        logger.warning(f"Skipping tick - missing symbol or price: {msg_value}")
        return
    
    window_start=(timestamp//WINDOW_SIZE_MS)*WINDOW_SIZE_MS
    key=(symbol, window_start)
    
    TICKS_PROCESSED.labels(symbol=symbol).inc()
    try:
        redis.set(f"ticker:{symbol}", price, ex=60)
    except Exception as e:
        logger.warning(f"Redis write failed for {symbol}: {e}")

    if key not in windows:
        windows[key]={
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volume
        }
    else:
        w=windows[key]
        w["high"]=max(w["high"], price)
        w["low"]=min(w["low"], price)
        w["close"]=price
        w["volume"]+=volume


def run():
    global running
    ensure_table()
    start_http_server(6066)
    logger.info("OHLCV agent started - metrics on port 6066")

    # check redis health
    try:
        redis.ping()
        logger.info("Redis connection OK")
    except Exception as e:
        logger.error(f"Redis connection failed: {e} - price cache will not work")

    flusher=threading.Thread(target=flush_loop, daemon=True)
    flusher.start()

    sr_client=SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_deserializer=AvroDeserializer(sr_client)

    consumer=DeserializingConsumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "ohlcv-agent",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "value.deserializer": avro_deserializer
    })
    consumer.subscribe(["raw.ticks"])

    def shutdown(signum, frame):
        global running
        logger.info("Shutting down OHLCV agent...")
        running=False
        flush_windows(force=True)
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
            if value:
                process_tick(value)
        except Exception as e:
            logger.error(f"Failed to process tick: {e}")

if __name__=="__main__":
    run()