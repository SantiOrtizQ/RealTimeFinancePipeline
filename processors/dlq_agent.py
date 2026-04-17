import os
import json
import signal
import logging

from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Consumer, Producer
from sqlalchemy import create_engine, text

from prometheus_client import Counter, start_http_server


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

KAFKA_BOOTSTRAP=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TIMESCALE_USER=os.getenv("TIMESCALE_USER", "financeuser")
TIMESCALE_PASS=os.getenv("TIMESCALE_PASSWORD", "financepass")
TIMESCALE_DB=os.getenv("TIMESCALE_DB", "financedb")
TIMESCALE_URL=f"postgresql://{TIMESCALE_USER}:{TIMESCALE_PASS}@localhost:5432/{TIMESCALE_DB}"


# prometheus metrics
DLQ_MESSAGES_RECEIVED=Counter(
    "dlq_messages_received_total",
    "Total messages recieved on the DLQ topic",
    ["source_topic"]
)
DLQ_MESSAGES_PERSISTED=Counter(
    "dlq_messages_persisted_total",
    "Total DLQ messages successfully persisted to TimescaleDB",
    ["source_topic"]
)



engine=create_engine(TIMESCALE_URL)
running=True


def ensure_table():
    with engine.connect() as conn:
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS dead_letter_queue (
                id SERIAL,
                raw_message TEXT NOT NULL,
                error TEXT NOT NULL,
                source_topic TEXT NOT NULL,
                failed_at TIMESTAMPTZ NOT NULL,
                reviewed BOOLEAN NOT NULL
            );
            """
        ))
        conn.commit()
    logger.info("dead_letter_queue table ready")


def insert_dlq_event(raw_message, error, source_topic, failed_at_ms):
    with engine.connect() as conn:
        conn.execute(text(
            """
            INSERT INTO dead_letter_queue
                (raw_message, error, source_topic, failed_at)
            VALUES
                (:raw_message, :error, :source_topic, to_timestamp(:failed_at/1000.0))
            """
        ), {
            "raw_message": raw_message,
            "error": error,
            "source_topic": source_topic,
            "failed_at": failed_at_ms
        })
        conn.commit()


def publish_to_dlq(raw_message: dict, error: str, source_topic: str):
    producer=Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    event=json.dumps({
        "raw_message": json.dumps(raw_message),
        "error": error,
        "source_topic": source_topic,
        "failed_at": int(datetime.now(timezone.utc).timestamp()*1000)
    })
    producer.produce(topic="raw.dlq", value=event.encode("utf-8"))
    producer.flush()
    logger.warning(f"Published to DLQ - topic={source_topic} error={error}")


def run():
    global running
    ensure_table()
    start_http_server(6069)
    logger.info("DLQ agent started")

    consumer=Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "dlq-agent",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    })
    consumer.subscribe(["raw.dlq"])

    def shutdown(signum, frame):
        global running
        logger.info("Shutting down DLQ agent...")
        running=False
        consumer.close()

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    while running:
        msg=consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Consumer error: {msg.error}")
            continue
        try:
            value=json.loads(msg.value().decode("utf-8"))
            
            # for prometheus metrics
            source_topic=value.get("source_topic", "unknown")
            DLQ_MESSAGES_RECEIVED.labels(source_topic=source_topic).inc()
            logger.warning(
                f"DLQ event | topic={value.get('source_topic')} | "
                f"error={value.get('error')} | "
                f"message={str(value.get('raw_message', ''))[:120]}"
            )
            insert_dlq_event(
                raw_message=value.get("raw_message", ""),
                error=value.get("error", ""),
                source_topic=source_topic,
                failed_at_ms=value.get("failed_at", 0)
            )
            DLQ_MESSAGES_PERSISTED.labels(source_topic=source_topic).inc()
        except Exception as e:
            logger.error(f"Failed to process DLQ message: {e}")


if __name__=="__main__":
    run()