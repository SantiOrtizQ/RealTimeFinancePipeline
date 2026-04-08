import os
import time
import hashlib
import logging
import signal
from datetime import datetime, timezone
from dotenv import load_dotenv
import requests
from producers.base_producer import BaseProducer

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SYMBOLS = ["AAPL", "TSLA", "MSFT", "GOOGL", "AMZN"]
POLL_INTERVAL_SECONDS = 900
NEWSAPI_URL = "https://newsapi.org/v2/everything"
running=True

class NewsProducer(BaseProducer):

    def __init__(self):
        super().__init__(
            topic="raw.news",
            schema_path="schemas/news_event.avsc",
        )
        self.api_key = os.getenv("NEWS_API_KEY")
        self.seen_ids = set()

    def _fetch_articles(self, symbol: str) -> list[dict]:
        try:
            response = requests.get(
                NEWSAPI_URL,
                params={
                    "q":        symbol,
                    "language": "en",
                    "sortBy":   "publishedAt",
                    "pageSize": 10,
                    "apiKey":   self.api_key,
                },
                timeout=10,
            )
            response.raise_for_status()
            return response.json().get("articles", [])
        except requests.exceptions.RequestException as e:
            logger.error(f"NewsAPI request failed for {symbol}: {e}")
            return []

    def _build_article_id(self, url: str) -> str:
        return hashlib.md5(url.encode()).hexdigest()

    def _parse_timestamp(self, published_at: str) -> int:
        try:
            dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
            return int(dt.timestamp() * 1000)
        except Exception:
            return int(datetime.now(timezone.utc).timestamp() * 1000)

    def _poll_once(self):
        logger.info(f"Polling NewsAPI for {len(SYMBOLS)} symbols...")
        published_count = 0

        for symbol in SYMBOLS:
            articles = self._fetch_articles(symbol)
            for article in articles:
                url = article.get("url", "")
                if not url:
                    continue

                article_id = self._build_article_id(url)
                if article_id in self.seen_ids:
                    continue

                self.seen_ids.add(article_id)

                record = {
                    "article_id":   article_id,
                    "title":        article.get("title") or "",
                    "description":  article.get("description"),
                    "source":       article.get("source", {}).get("name") or "",
                    "url":          url,
                    "published_at": self._parse_timestamp(
                                        article.get("publishedAt", "")
                                    ),
                    "symbols":      [symbol],
                }

                self.publish(record=record, key=article_id)
                published_count += 1
                logger.info(f"Published article: {record['title'][:60]}...")

        self.flush()
        logger.info(f"Poll complete — {published_count} new articles published")

    def run(self):
        global running
        logger.info(f"Starting News producer — polling every {POLL_INTERVAL_SECONDS}s")

        try:
            while running:
                try:
                    self._poll_once()
                except Exception as e:
                    logger.error(f"Poll cycle failed: {e}")
                finally:
                    logger.info(f"Sleeping {POLL_INTERVAL_SECONDS}s until next poll...")
                    time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Keyboard interrupt detected.")
        finally:
            logger.info("Cleaning up resources...")
            self.flush()
            logger.info("Producer shut down successfully.")


if __name__ == "__main__":
    producer = NewsProducer()
    producer.run()