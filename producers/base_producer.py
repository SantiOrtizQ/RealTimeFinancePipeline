import os
import logging
import time
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField


# create logger
logger=logging.getLogger(__name__)

# create class BaseProducer
class BaseProducer:

    def __init__(self, topic: str, schema_path: str):
        self.topic=topic
        self.schema_str=self._load_schema(schema_path)
        self.producer=self._build_producer()
        self.serializer=self._build_serializer()
    
    def _load_schema(self, schema_path: str) -> str:
        base_dir=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        full_path=os.path.join(base_dir, schema_path)
        with open(full_path, "r") as f:
            return f.read()
    
    def _build_producer(self) -> Producer:
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        return Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 500
        })
    
    def _build_serializer(self) -> AvroSerializer:
        registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
        registry_client=SchemaRegistryClient({"url": registry_url})
        return AvroSerializer(
            schema_registry_client=registry_client,
            schema_str=self.schema_str
        )
    
    def _delivery_report(self, err, msg):
        if err:
            logger.error(f"Delivery failed for topic {msg.topic()}: {err}")
        else:
            logger.debug(f"Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")
        
    def publish(self, record: dict, key: str=None, retries: int=3, delay: float=1.0):
        for attempt in range(retries):
            try:
                self.producer.produce(
                    topic=self.topic,
                    key=key,
                    value=self.serializer(
                        record,
                        SerializationContext(self.topic, MessageField.VALUE)
                    ),
                    on_delivery=self._delivery_report
                )
                self.producer.poll(0)
                return
            except Exception as e:
                logger.warning(f"Publish attempt {attempt+1} failed: {e}")
                if attempt<retries-1:
                    time.sleep(delay*(attempt+1))
                else:
                    logger.error(f"All {retries} publish attempts failed for record: {record}")
                    raise
    
    def flush(self):
        self.producer.flush()
        logger.info(f"Producer flushed for topic {self.topic}")