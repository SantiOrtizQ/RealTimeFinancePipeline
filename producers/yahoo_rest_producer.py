import os
import time
import logging

from datetime import datetime, timezone
from dotenv import load_dotenv
import yfinance as yf
from producers.base_producer import BaseProducer

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

SYMBOLS=list(os.getenv("SYMBOLS").split(","))
POLL_INTERVAL_SECONDS=300
running=True

#create class for Yahoo Producer
class YahooRestProducer(BaseProducer):
    def __init__(self):
        super().__init__(
            topic="raw.ticks",
            schema_path="schemas/tick_event.avsc"
        )
    
    def _fetch_ticker(self, symbol: str) -> dict | None:
        try:
            ticker=yf.Ticker(symbol)
            info=ticker.fast_info

            price=getattr(info, "last_price", None)
            volume=getattr(info, "last_volume", None)

            if price is None or volume is None:
                logger.warning(f"Incomplete data for {symbol} - skipping")
                return None
            
            return {
                "symbol": symbol,
                "price": float(price),
                "bid": float(getattr(info, "bid", 0.0) or 0.0),
                "ask": float(getattr(info, "ask", 0.0) or 0.0),
                "volume": int(volume),
                "timestamp": int(datetime.now(timezone.utc).timestamp()*1000),
                "source": "rest",
                "bid_size": None
            }
        except Exception as e:
            logger.error(f"Failed to fetch {symbol} from Yahoo Finance: {e}")
            return None
    
    def _poll_once(self):
        logger.info(f"Polling Yahoo Finance for {len(SYMBOLS)} symbols...")
        success_count=0
        for symbol in SYMBOLS:
            record=self._fetch_ticker(symbol)
            if record:
                self.publish(record=record, key=symbol)
                logger.info(f"Published REST tick: {symbol} @ {record['price']}")
                success_count+=1
        
        self.flush()
        logger.info(f"Poll complete - {success_count}/{len(SYMBOLS)} symbols published")
    
    def run(self):
        global running
        logger.info(f"Starting Yahoo REST producer - polling every {POLL_INTERVAL_SECONDS}s")
        try:
            while running:
                try:
                    self._poll_once()
                except Exception as e:
                    logger.error(f"Poll cycle failed: {e}")
                finally:
                    logger.info(f"Sleeping {POLL_INTERVAL_SECONDS}s until next poll")
                    time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("Keyboard interruption detected.")
        finally:
            logger.info("Cleaning up resources...")
            self.flush()
            logger.info("Producer shut down successfully.")
    

# Initialize Yahoo Producer
if __name__=="__main__":
    producer=YahooRestProducer()
    producer.run()
