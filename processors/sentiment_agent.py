import os
import logging
from dotenv import load_dotenv
import faust
from sqlalchemy import create_engine, text
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from prometheus_client import Counter, start_http_server

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)


KAFKA_BOOTSTRAP=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TIMESCALE_USER=os.getenv("TIMESCALE_USER", "financeuser")
TIMESCALE_PASS=os.getenv("TIMESCALE_PASSWORD", "financepass")
TIMESCALE_DB=os.getenv("TIMESCALE_DB", "financedb")
TIMESCALE_URL=f"postgresql://{TIMESCALE_USER}:{TIMESCALE_PASS}@localhost:5432/{TIMESCALE_DB}"

app=faust.App(
    "sentiment-agent",
    broker=f"kafka://{KAFKA_BOOTSTRAP}",
    value_serializer="json"
)

ARTICLES_SCORED=Counter(
    "sentiment_articles_scored_total",
    "Total articles scored by the sentiment agent",
    ["sentiment"]
)

analyzer=SentimentIntensityAnalyzer()


class NewsEvent(faust.Record):
    article_id: str
    title: str
    source: str
    url: str
    published_at: int
    description: str | None=None
    symbols: list=None


news_topic=app.topic("raw.news", value_type=NewsEvent)
engine=create_engine(TIMESCALE_URL)



def ensure_table():
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS news_sentiment (
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
        """))
        conn.execute(text("""
            SELECT create_hypertable(
                          'news_sentiment', 'published_at',
                          if_not_exists=>TRUE
            );
        """))
        conn.commit()
    logger.info("news_sentiment hypertable ready")



def score_sentiment(title: str, description: str=None) -> dict:
    text_to_score=title
    if description:
        text_to_score=f"{title}.\n{description}"
    
    scores=analyzer.polarity_scores(text_to_score)

    compound=scores["compound"]
    if compound>=0.05:
        label="positive"
    elif compound<=-0.05:
        label="negative"
    else:
        label="neutral"
    
    return {
        "compound": compound,
        "positive": scores["pos"],
        "neutral": scores["neu"],
        "negative": scores["neg"],
        "sentiment": label
    }


def insert_sentiment(event: NewsEvent, scores: dict):
    ARTICLES_SCORED.labels(sentiment=scores["sentiment"]).inc()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO news_sentiment
                (article_id, title, source, url, symbols, compound, positive, neutral, negative, sentiment, published_at)
            VALUES
                (:article_id, :title, :source, :url, :negative, :compound
                    :positive, :neutral, :negative, :sentiment,
                    to_timestamp(:published_at/1000.0))
            ON CONFLICT DO NOTHING
        """), {
            "article_id": event.article_id,
            "title": event.title,
            "source": event.source,
            "url": event.url,
            "symbols": event.symbols or [],
            "compound": scores["compound"],
            "positive": scores["positive"],
            "neutral": scores["neutral"],
            "negative": scores["negative"],
            "sentiment": scores["sentiment"],
            "published_at": event.published_at
        })
        conn.commit()


async def score_news(events):
    async for event in events:
        try:
            scores=score_sentiment(
                title=event.title,
                description=event.description
            )

            insert_sentiment(event, scores)

            logger.info(
                f"Scored: [{scores['sentiment'].upper():8s}] "
                f"compound={scores['compound']:+.3f} | "
                f"{event.title[:60]}"
            )
        except Exception as e:
            logger.error(f"Failed to score article {event.article_id}: {e}")


@app.task
async def on_started(app, **kwargs):
    start_http_server(6068)
    ensure_table()
    logger.info("Sentiment agent started")


if __name__=="__main__":
    app.main()