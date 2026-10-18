import pytest
import pandas as pd
import requests
from unittest.mock import MagicMock, call
from src.ingestion.kafka import KafkaConsumer, KafkaProducer, OilPriceFetcher, run_oil_price_ingestion
from confluent_kafka import KafkaException


def test_kafka_consumer_success():

    fake_msg_1 = MagicMock()
    fake_msg_1.error.return_value = None
    fake_msg_1.key.return_value = b"BRENT_CRUDE_USD"
    fake_msg_1.value.return_value = b'{"status": "success"}'

    fake_msg_2 = MagicMock()
    fake_msg_2.error.return_value = None
    fake_msg_2.key.return_value = b"WTI_USD"
    fake_msg_2.value.return_value = b'{"status": "success"}'

    fake_consumer_instance = MagicMock()
    fake_consumer_instance.consume.return_value = [fake_msg_1, fake_msg_2]
    mock_consumer_class = MagicMock(return_value=fake_consumer_instance)
    mock_config = {"bootstrap.servers": "fake"}

    result = KafkaConsumer(mock_consumer_class, mock_config)
    result = result.fetch("test_consumer", "test_group_id", "earliest")

    assert len(result) == 2
    assert list(result.columns)== ["key", "value"]


def test_msg_error_continues_to_next_iteration_consumer():

    fake_msg_1 = MagicMock()
    fake_msg_1.error.return_value = "continue"
    fake_msg_1.key.return_value = b"BRENT_CRUDE_USD"
    fake_msg_1.value.return_value = b'{"status": "success"}'

    fake_msg_2 = MagicMock()
    fake_msg_2.error.return_value = "continue"
    fake_msg_2.key.return_value = b"WTI_USD"
    fake_msg_2.value.return_value = b'{"status": "success"}'

    fake_consumer_instance = MagicMock()
    fake_consumer_instance.consume.return_value = [fake_msg_1, fake_msg_2]
    mock_consumer_class = MagicMock(return_value=fake_consumer_instance)
    mock_config = {"bootstrap.servers": "fake"}

    result = KafkaConsumer(mock_consumer_class, mock_config)
    result = result.fetch("test_consumer", "test_group_id", "earliest")

    assert len(result) == 0


def test_kafka_consumer_exception():

    fake_consumer_instance = MagicMock()
    fake_consumer_instance.consume.side_effect = KafkaException("broker unreachable")
    mock_consumer_class = MagicMock(return_value=fake_consumer_instance)
    mock_config = {"bootstrap.servers": "fake"}

    result = KafkaConsumer(mock_consumer_class, mock_config)
    result = result.fetch("test_consumer", "test_group_id", "earliest")

    assert result is None


def test_unicode_error_consumer():

    fake_msg_1 = MagicMock()
    fake_msg_1.error.return_value = None
    fake_msg_1.key.return_value = b"BRENT_CRUDE_USD"
    fake_msg_1.value.return_value = b'\xff\xfe'

    fake_consumer_instance = MagicMock()
    fake_consumer_instance.consume.return_value = [fake_msg_1]
    mock_consumer_class = MagicMock(return_value=fake_consumer_instance)
    mock_config = {"bootstrap.servers": "fake"}

    result = KafkaConsumer(mock_consumer_class, mock_config)
    result = result.fetch("test_consumer", "test_group_id", "earliest")

    assert result is None


def test_kafka_consumer_unexpected_exception():

    fake_consumer_instance = MagicMock()
    fake_consumer_instance.consume.side_effect = TypeError("There's a bug in the code")
    mock_consumer_class = MagicMock(return_value=fake_consumer_instance)
    mock_config = {"bootstrap.servers": "fake"}

    result = KafkaConsumer(mock_consumer_class, mock_config)
    # result = result.fetch("test_consumer", "test_group_id", "earliest")

    with pytest.raises(TypeError):
        result.fetch("test_consumer", "test_group_id", "earliest")


def test_kafka_producer_send():
    fake_producer_instance = MagicMock()
    mock_producer_class = MagicMock(return_value=fake_producer_instance)
    mock_config = {"bootstrap.servers": "fake"}

    producer = KafkaProducer(mock_producer_class, mock_config, "fake_topic")
    producer.send("BRENT_CRUDE_USD", {"status": "success"})

    fake_producer_instance.produce.assert_called_once_with("fake_topic", key="BRENT_CRUDE_USD", value=b'{"status": "success"}')


def test_kafka_producer_flush():
    fake_producer_instance = MagicMock()
    mock_producer_class = MagicMock(return_value=fake_producer_instance)
    mock_config = {"bootstrap.servers": "fake"}

    producer = KafkaProducer(mock_producer_class, mock_config, "fake_topic")
    producer.send("BRENT_CRUDE_USD", {"status": "success"})
    
    fake_producer_instance.flush.assert_not_called() 
    producer.flush()
    fake_producer_instance.flush.assert_called_once()


def test_oil_price_fetcher_success():
    fake_response = MagicMock()
    fake_response.json.return_value = {"date": "2026-06-06", "code": "BRENT_CRUDE_USD", "price": "£27.57"}
    fake_response.status_code = 200

    mock_session = MagicMock()
    mock_session.get.return_value = fake_response

    fetcher = OilPriceFetcher(session= mock_session, api_key="fake_key", api_url="http://fake-url")
    result = fetcher.fetch("BRENT_CRUDE_USD")

    assert result == {"date": "2026-06-06", "code": "BRENT_CRUDE_USD", "price": "£27.57"}


def test_oil_price_fetcher_returns_none():
    fake_response = MagicMock()
    fake_response.status_code = 404

    mock_session = MagicMock()
    mock_session.get.return_value = fake_response

    fetcher = OilPriceFetcher(session= mock_session, api_key="fake_key", api_url="http://fake-url")
    result = fetcher.fetch("BRENT_CRUDE_USD")

    assert result is None


def test_oil_price_fetcher_connection_error():
    mock_session = MagicMock()
    mock_session.get.side_effect = requests.exceptions.ConnectionError("Connection error")

    fetcher = OilPriceFetcher(session=mock_session, api_key="fake_key", api_url="http://fake-url")
    result = fetcher.fetch("BRENT_CRUDE_USD")

    assert result is None


def test_oil_price_fetcher_request_exception():
    mock_session = MagicMock()
    mock_session.get.side_effect = requests.exceptions.RequestException("Error fetching oil prices")

    fetcher = OilPriceFetcher(session=mock_session, api_key="fake_key", api_url="http://fake-url")
    result = fetcher.fetch("BRENT_CRUDE_USD")

    assert result is None


def test_oil_price_unexpected_exception():
    mock_session = MagicMock()
    mock_session.get.side_effect = TypeError("There's a bug in the code")

    fetcher = OilPriceFetcher(session=mock_session, api_key="fake_key", api_url="http://fake-url")

    with pytest.raises(TypeError):
        fetcher.fetch("BRENT_CRUDE_USD")


def test_run_op_ingestion_success():
    fetcher = MagicMock()
    fetcher.fetch.return_value = {"status": "success"}
    producer = MagicMock()

    run_oil_price_ingestion(fetcher, producer, ["BRENT_CRUDE_USD", "WTI_USD", "NATURAL_GAS_USD"])

    fetcher.send.call_count = 3
    producer.flush.assert_called_once()


def test_run_op_ingestion_does_not_send_none_values():
    fetcher = MagicMock()
    fetcher.fetch.side_effect = [{"a": 1}, None, {"c": 3}]
    producer = MagicMock()

    run_oil_price_ingestion(fetcher, producer, ["BRENT_CRUDE_USD", "WTI_USD", "NATURAL_GAS_USD"])

    fetcher.send.call_count = 2
    producer.flush.assert_called_once()
    
    assert producer.send.call_args_list == [call("BRENT_CRUDE_USD", {"a": 1}), call("NATURAL_GAS_USD", {"c": 3})]


def test_run_op_ingestion_returns_empty_dict():
    fetcher = MagicMock()
    fetcher.fetch.return_value = {}
    producer = MagicMock()

    run_oil_price_ingestion(fetcher, producer, ["BRENT_CRUDE_USD"])

    producer.send.assert_called_once_with("BRENT_CRUDE_USD", {})
    producer.flush.assert_called_once()
    