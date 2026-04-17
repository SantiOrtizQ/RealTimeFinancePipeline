import os
import json
import logging
import time

from datetime import datetime
from dotenv import load_dotenv
from websocket import WebSocketApp
from producers.base_producer import BaseProducer

from processors.dlq_agent import publish_to_dlq

load_dotenv()
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

SYMBOLS=list(os.getenv("SYMBOLS").split(","))
running=True

WS_URL=os.getenv("WS_URL", "wss://stream.data.alpaca.markets/v2/iex")


# create class for alpaca
class AlpacaWsProducer(BaseProducer):

    def __init__(self):
        super().__init__(
            topic="raw.ticks",
            schema_path="schemas/tick_event.avsc"
        )
        self.api_key=os.getenv("ALPACA_API_KEY")
        self.api_secret=os.getenv("ALPACA_API_SECRET")

    def _on_open(self, ws):
        logger.info("Websocket connection opened")
        auth_msg={
            "action": "auth",
            "key": self.api_key,
            "secret": self.api_secret
        }
        ws.send(json.dumps(auth_msg))
    
    def _on_message(self, ws, message):
        events=json.loads(message)
        for event in events:
            msg_type=event.get("T")

            if msg_type=="success":
                if event.get("msg")=="authenticated":
                    logger.info("Authenticated with Alpaca - subscribing to symbols")
                    sub_msg={
                        "action": "subscribe",
                        "trades": SYMBOLS
                    }
                    ws.send(json.dumps(sub_msg))
            elif msg_type=="t":
                self._handle_trade(event)
    
    def _handle_trade(self, event: dict):
        try:
            record={
                "symbol": event["S"],
                "price": float(event["p"]),
                "bid": 0.0,
                "ask": 0.0,
                "volume": int(event["s"]),
                "timestamp": int(datetime.fromisoformat(
                    event["t"].replace("Z", "+00:00")
                ).timestamp()*1000),
                "source": "websocket",
                "bid_size": None
            }
            self.publish(record=record, key=record["symbol"])
            logger.info(f"Published tick: {record['symbol']} @ {record['price']}")
        except Exception as e:
            logger.error(f"Failed to handle trade event: {e} | raw: {event}")
            publish_to_dlq(
                raw_message=event,
                error=str(e),
                source_topic="raw.ticks"
            )
    
    def _on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        global running
        logger.warning(f"WebSocket closed: {close_status_code} - {close_msg}")
        running=False
    
    def run(self, reconnect_delay:float=5.0):
        global running

        while running:
            try:
                logger.info("Connecting to Alpaca WebSocket...")
                ws=WebSocketApp(
                    WS_URL,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                ws.run_forever()
            except KeyboardInterrupt:
                running=False
                logger.info("Cleaning up resources...")
                self.flush()
                logger.info("Producer shut down successfully.")
            except Exception as e:
                logger.error(f"Connection failed: {e}")
                self.flush()
                logger.info(f"Reconnecting in {reconnect_delay}s...")
                time.sleep(reconnect_delay)

if __name__=="__main__":
    producer=AlpacaWsProducer()
    producer.run()