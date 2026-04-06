import os
import json
import signal
import logging
from dotenv import load_dotenv
from confluent_kafka import Consumer
from sqlalchemy import create_engine, text
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from prometheus_client import Counter, start_http_server

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

KAFKA_BOOTSTRAP=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TIMESCALE_USER=os.getenv("TIMESCALE_USER", "financeuser")
TIMESCALE_PASS=os.getenv("TIMESCALE_PASSWORD", "financepass")
TIMESCALE_DB=os.getenv("TIMESCALE_DB", "financedb")
TIMESCALE_URL=f"postgresql://{TIMESCALE_USER}:{TIMESCALE_PASS}@localhost:5432/{TIMESCALE_DB}"

engine=create_engine(TIMESCALE_URL)
analyzer=SentimentIntensityAnalyzer()
running=True

ARTICLES_SCORED=Counter(
    "sentiment_articles_scored_total",
    "Articles scored",
    ["sentiment"]
)


def ensure_table():
    with engine.connect() as conn:
        conn.execute(text(
            """
            CREATE TABLE IF NOT EXISTS news_sentiment(
                article_id TEXT NOT NULL,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                url TEXT NOT NULL,
                symbols TEXT[] NOT NULL,
                compound DOUBLE PRECISION NOT NULL,
                positive DOUBLE PRECISION NOT NULL,
                neutral DOUBLE PRECISION NOT NULL,
                negative DOUBLE PRECISION NOT NULL,
                sentiment TEXT NOT NULL,
                published_at TIMESTAMPTZ NOT NULL
            );
            """
        ))
        conn.execute(text(
            """
            SELECT create_hypertable(
                'news_sentiment', 'published_at',
                if_not_exists=>TRUE
            );
            """
        ))
        conn.commit()
    logger.info("news_sentiment hypertable ready")



def score_sentiment(title:str, description: str=None) -> dict:
    text_to_score=title
    if description:
        text_to_score=f"{title}. {description}"
    scores=analyzer.polarity_scores(text_to_score)
    compound=scores["compound"]
    label="positive" if compound >= 0.05 else "negative" if compound<=-0.05 else "neutral"
    return {
        "compound": compound,
        "positive": scores["pos"],
        "neutral": scores["neu"],
        "negative": scores["neg"],
        "sentiment": label
    }


def insert_sentiment(event: dict, scores: dict):
    with engine.connect() as conn:
        conn.execute(text(
            """
            INSERT INTO news_sentiment
                (article_id, title, source, url, symbols,
                compound, positive, neutral, negative, sentiment,
                published_at)
            VALUES
                (:article_id, :title, :source, :url, :symbols,
                :compound, :positive, :neutral, :negative,
                :sentiment, to_timestamp(:published_at/1000.0))
            ON CONFLICT DO NOTHING
            """
        ), {
            "article_id": event.get("article_id"),
            "title": event.get("title", ""),
            "source": event.get("source", ""),
            "url": event.get("url", ""),
            "symbols": event.get("symbols") or [],
            "compound": event.get("compound"),
            "positive": event.get("positive"),
            "neutral": event.get("neutral"),
            "negative": event.get("negative"),
            "sentiment": event.get("sentiment"),
            "published_at": event.get("published_at")
        })
        conn.commit()


def process_article(msg_value: dict):
    try:
        scores=score_sentiment(
            title=msg_value.get("title", ""),
            description=msg_value.get("description")
        )
        insert_sentiment(msg_value, scores)
        ARTICLES_SCORED.labels(sentiment=scores["sentiment"]).inc()
        logger.info(
            f"Scored: [{scores['sentiment'].upper():8s}] "
            f"compound={scores['compound']:+.3f} | "
            f"{msg_value.get('title', ''[:60])}..."
        )
    except Exception as e:
        logger.error(f"Failed to score article: {e}")


def run():
    global running
    ensure_table()
    start_http_server(6068)
    logger.info("Sentiment agent started - metrics on port 6068")

    consumer=Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": "sentiment-agent",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True
    })
    consumer.subscribe(["raw.news"])

    def shutdown(signum, frame):
        global running
        logger.info("Shutting down sentiment agent...")
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
            value=json.loads(msg.value().decode("utf-8"))
            process_article(value)
        except Exception as e:
            logger.error(f"Failed to process message: {e}")


if __name__=="__main__":
    run()