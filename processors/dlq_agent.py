import os
import json
import logging
from datetime import datetime, timezone
from dotenv import load_dotenv
import faust
from sqlalchemy import create_engine, text

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

KAFKA_BOOTSTRAP=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TIMESCALE_USER=os.getenv("TIMESCALE_USER", "financeuser")
TIMESCALE_PASS=os.getenv("TIMESCALE_PASSWORD", "financepass")
TIMESCALE_DB=os.getenv("TIMESCALE_DB", "financedb")
TIMESCALE_URL=f"postgresql://{TIMESCALE_USER}:{TIMESCALE_PASS}@localhost:5432/{TIMESCALE_DB}"


app=faust.App(
    "dlq-agent",
    broker=f"kafka://{KAFKA_BOOTSTRAP}",
    value_serializer="json"
)


class DlqEvent(faust.Record, serializer="json"):
    raw_message: str
    error: str
    source_topic: str
    failed_at: int



dlq_topic=app.topic("raw.dlq", value_type=DlqEvent)

engine=create_engine(TIMESCALE_URL)


def ensure_table():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dead_letter_queue (
                id SERIAL,
                raw_message TEXT NOT NULL,
                error TEXT NOT NULL,
                source_topic TEXT NOT NULL,
                failed_at TIMESTAMPTZ NOT NULL,
                reviewed BOOLEAN NOT NULL DEFAULT FALSE
            );
        """))
        conn.commit()
    logger.info("dead_letter_queue table ready")


def insert_dlq_event(event: DlqEvent):
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO dead_letter_queue
                (raw_message, error, source_topic, failed_at)
            VALUES
                (:raw_message, :error, :source_topic, to_timestamp(:failed_at/1000.0))
        """), {
            "raw_message": event.raw_message,
            "error": event.error,
            "source_topic": event.source_topic,
            "failed_at": event.failed_at
        })
        conn.commit()



def publish_to_dlq(raw_message: dict, error: str, source_topic: str, dlq_producer):
    try:
        event={
            "raw_message": json.dumps(raw_message),
            "error": error,
            "source_topic": source_topic,
            "failed_at": int(datetime.now(timezone.utc).timestamp*1000)
        }
        dlq_producer.publish(record=event, key=source_topic)
        logger.warning(
            f"Published to DLQ - topic={source_topic} error={error}"
        )
    except Exception as e:
        logger.error(f"Failed to publish to DLQ: {e}")


@app.agent(dlq_topic)
async def process_dlq(events):
    async for event in events:
        logger.warning(
            f"DLQ event received | "
            f"topic={event.source_topic} | "
            f"error={event.error} | "
            f"message={event.raw_message[:120]}..."
        )
        try:
            insert_dlq_event(event)
        except Exception as e:
            logger.error(f"Failed to persist DLQ event: {e}")


@app.task
async def on_started(app, **kwargs):
    ensure_table()
    logger.info("DLQ agent started")


if __name__=="__main__":
    app.main()