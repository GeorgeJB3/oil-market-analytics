import pandas as pd
import logging
import json
import requests
from typing import Optional
from confluent_kafka import Consumer, Producer, KafkaException

from src.ingestion.http_utils import handle_status

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """
    Consumes messages from a Kafka topic and returns them as a DataFrame.

    A Consumer class (not an instance) is injected via `consumer_class`
    so a fresh consumer can be built per topic inside `fetch()`, and
    swapped for a mock in tests.
    """
    def __init__(self, consumer_class: type[Consumer], consumer_config:dict):
        """
        Args:
            consumer_class (type[Consumer]): Consumer class to instantiate.
            consumer_config (dict): Base Kafka client config.
        """
        self.consumer_class = consumer_class
        self.consumer_config = consumer_config
    

    def _consume_message(self, consumer) -> pd.DataFrame:
        """
        Consume up to 50 messages within a 10s timeout, skipping any
        message with a Kafka-level error. Errors are logged and result
        in None; the consumer is always closed afterwards.

        Args:
            consumer (Consumer): An active, subscribed consumer.

        Returns:
            pandas.DataFrame or None: Decoded key/value messages, or
            None on error.
        """
        try:
            messages = consumer.consume(num_messages=50, timeout=10.0)
            results = []

            for msg in messages:
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue
                key = msg.key().decode("utf-8") if msg.key() else None
                value = msg.value().decode("utf-8")
                results.append({"key": key, "value": value})

            consumed_df = pd.DataFrame(results) 
            return consumed_df   

        except KafkaException as e:
            logger.error(f"Kafka error while consuming: {e}")    
            return None
        except UnicodeDecodeError as e:
            logger.error(f"Malformed message payload: {e}")
            return None

        finally:
            consumer.close()


    def fetch(self, topic: str, group_id: str, auto_offset_reset: str, auto_commit: bool=False) -> pd.DataFrame:
        """
        Build a consumer for the given topic and consume its messages.

        Args:
            topic (str): Kafka topic to consume from.
            group_id (str): Consumer group ID.
            auto_offset_reset (str): Offset reset policy.
            auto_commit (bool, optional): Auto-commit offsets. Defaults to False.

        Returns:
            pandas.DataFrame or None: Consumed messages, or None on error.
        """
        config = dict(self.consumer_config)

        ######## prod ########
        # config["group.id"] = group_id # "airflow-batch-consumer"
        # config["auto.offset.reset"] = auto_offset_reset # "earliest"
        # config["enable.auto.commit"] = auto_commit # True 

        ########### for testing ############
        config["group.id"] = group_id # "test-consumer"
        config["auto.offset.reset"] = auto_offset_reset # "earliest"
        config["enable.auto.commit"] = auto_commit 

        consumer = self.consumer_class(config)
        consumer.subscribe([topic])

        return self._consume_message(consumer)


class KafkaProducer:
    """
    Publishes messages to a Kafka topic.

    A Producer class (not an instance) is injected via `producer_class`
    so it can be swapped for a mock in tests. One producer is built at
    construction and reused for every send.
    """

    def __init__(self, producer_class: type[Producer], producer_config:dict, topic: str):
        """
        Args:
            producer_class (type[Producer]): Producer class to instantiate.
            producer_config (dict): Kafka client config.
            topic (str): Topic that messages are published to.
        """
        self.producer_class = producer_class
        self.producer_config = producer_config
        self.topic = topic
        self.producer = self.producer_class(dict(self.producer_config))

    def send(self,  code: str, value:dict) -> None:
        """Publish one value to Kafka. Does not flush — caller flushes after the batch."""
        
        value = json.dumps(value).encode("utf-8")
        self.producer.produce(self.topic, key=code, value=value)
        logger.info(f"Produced {code} to {self.topic}.")
        
    def flush(self):
        """Flush all buffered messages."""
        self.producer.flush()


class OilPriceFetcher:
    """
    Fetches commodity price data from the OilPrice API.

    An HTTP session is injected so it can be swapped for a mock in tests.
    """

    def __init__(self, session: requests.Session, api_key: str, api_url: str):
        """
        Args:
            session (requests.Session): HTTP session used for requests.
            api_key (str): API token for the Authorization header.
            api_url (str): Base URL of the prices endpoint.
        """
        self.session = session
        self.api_key = api_key
        self.api_url = api_url
    

    def fetch(self, code: str) -> Optional[dict]:
        """
        Fetch price data for one commodity code.

        Non-200 statuses and request errors are logged and return None.
        Any other exception propagates.

        Args:
            code (str): Commodity code (e.g. "BRENT_CRUDE_USD").

        Returns:
            dict or None: Parsed JSON response, or None on failure.
        """
        headers = {
            "Authorization": f"Token {self.api_key}",
            'Content-Type': 'application/json'
        }

        try:
            response = self.session.get(f"{self.api_url}?by_code={code}", headers=headers)
            if response.status_code == 200:
                logger.info(f"Successfully fetched {code} prices.")
                value = response.json()

                return value

            else:
                handle_status(code, response.status_code, logger)
                return None
        
        except requests.exceptions.ConnectionError as ce:
            logger.error(f"Connection error: {ce}")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching oil prices: {e}")
            return None
        

def run_oil_price_ingestion(fetcher: OilPriceFetcher, producer: KafkaProducer, codes: list[str]):
    """
    Fetch and publish oil prices for each given commodity code.

    Args:
        fetcher (OilPriceFetcher): Fetches price data per code.
        producer (KafkaProducer): Publishes fetched data to Kafka.
        codes (list[str]): Commodity codes to fetch and publish.
    """
    for code in codes:
        data = fetcher.fetch(code)
        if data is not None:
            producer.send(code, data)
    producer.flush()
