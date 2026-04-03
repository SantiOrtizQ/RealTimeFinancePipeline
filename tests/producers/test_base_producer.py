import pytest
from unittest.mock import MagicMock, patch, mock_open
import json

MOCK_SCHEMA=json.dumps({
    "type": "record",
    "name": "TestEvent",
    "namespace": "com.test",
    "fields": [
        {"name": "symbol", "type": "string"},
        {"name": "price", "type": "double"}
    ]
})


@patch("producers.base_producer.AvroSerializer")
@patch("producers.base_producer.SchemaRegistryClient")
@patch("producers.base_producer.Producer")
@patch("builtins.open", mock_open(read_data=MOCK_SCHEMA))
def test_producer_initializes(mock_producer, mock_registry, mock_serializer):
    from producers.base_producer import BaseProducer
    producer=BaseProducer(topic="raw.ticks", schema_path="schemas/tick_event.avsc")
    assert producer.topic=="raw.ticks"
    assert producer.schema_str==MOCK_SCHEMA



@patch("producers.base_producer.AvroSerializer")
@patch("producers.base_producer.SchemaRegistryClient")
@patch("producers.base_producer.Producer")
@patch("builtins.open", mock_open(read_data=MOCK_SCHEMA))
def test_publish_calls_produce(mock_producer, mock_registry, mock_serializer):
    from producers.base_producer import BaseProducer
    producer=BaseProducer(topic="raw.ticks", schema_path="schemas/tick_event.avsc")
    producer.serializer=MagicMock(return_value=b"serialized")
    record={"symbol": "AAPL", "price": 150.0}
    producer.publish(record=record, key="AAPL")
    producer.producer.produce.assert_called_once()


@patch("producers.base_producer.AvroSerializer")
@patch("producers.base_producer.SchemaRegistryClient")
@patch("producers.base_producer.Producer")
@patch("builtins.open", mock_open(read_data=MOCK_SCHEMA))
def test_publish_retries_on_failure(mock_producer, mock_registry, mock_serializer):
    from producers.base_producer import BaseProducer

    producer=BaseProducer(topic="raw.ticks", schema_path="schemas/tick_event.avsc")
    producer.serializer=MagicMock(return_value=b"serialized")
    producer.producer.produce=MagicMock(side_effect=Exception("Broker unavailable"))

    with patch("time.sleep"):
        with pytest.raises(Exception, match="Broker unavailable"):
            producer.publish(record={"symbol": "AAPL", "price": 150.0})
    
    assert producer.producer.produce.call_count==3, (
        "Should retry exactly 3 times before raising"
    )



@patch("producers.base_producer.AvroSerializer")
@patch("producers.base_producer.SchemaRegistryClient")
@patch("producers.base_producer.Producer")
@patch("builtins.open", mock_open(read_data=MOCK_SCHEMA))
def test_delivery_report_logs_error(mock_producer, mock_registry, mock_serializer, caplog):
    from producers.base_producer import BaseProducer
    import logging

    producer=BaseProducer(topic="raw.ticks", schema_path="schemas/tick_event.avsc")
    mock_msg=MagicMock()
    mock_msg.topic.return_value="raw.ticks"
    
    with caplog.at_level(logging.ERROR):
        producer._delivery_report(err="some error", msg=mock_msg)
    
    assert "Delivery failed" in caplog.text