import requests
import logging
import json

from confluent_kafka import Producer

from src.ingestion.load_config import load_config, read_client


def handle_status(code, status_code, logger): 
  """
    Log and handle API response status codes.

    Maps common HTTP error codes to messages and logs them 
    with the provided logger. If the status code is not in
    the predefined set, a generic "Unexpected error" message is
    logged instead.

    Args:
        code (str): Identifier for the API request (e.g., commodity code).
        status_code (int): HTTP status code returned by the API.
        logger (logging.Logger): Logger instance used to record errors.
  """
  
  messages = {
      400: "Bad Request (invalid parameters).",
      401: "Unauthorized (invalid API key).",
      429: "Too Many Requests (rate limit exceeded).",
      500: "Internal Server Error.",
  }

  msg = messages.get(status_code, f"Unexpected error: {status_code}.")
  logger.error(f"{status_code} {msg} for {code}.")

  return None


def fetch_oil_prices(spark, topic="energy_prices"):
  """
    Retrieve energy price data from the OilPrice API and publish to Kafka.

    Fetches WTI crude oil, Brent crude oil, and natural gas prices
    using the configured API credentials. Each response is serialized
    to JSON and sent to the specified Kafka topic. Handles connection
    and request errors, logging issues as they occur.

    Args:
        spark: Active Spark session (not directly used,
            but included for consistency with pipeline functions).
        topic: Kafka topic to publish messages to. 
            Defaults to "energy_prices".
  """
  logger = logging.getLogger(__name__)

  # Kafka config
  kafka_config = read_client()
  producer = Producer(kafka_config)

  # API config
  api_config = load_config()
  API_KEY = api_config["oil_price_api"]["key"]
  API_URL = api_config["oil_price_api"]["url"]
  HEADERS = {
    "Authorization": f"Token {API_KEY}",
    'Content-Type': 'application/json'
  }

  codes = [
    api_config["oil_price_api"]["brent"], 
    api_config["oil_price_api"]["wti"], 
    api_config["oil_price_api"]["natural_gas"]
    ]
  
  # Invoke API call for each code: BRENT_CRUDE_USD / WTI_USD / NATURAL_GAS_USD
  for code in codes:
    try:
      response = requests.get(f"{API_URL}?by_code={code}", headers=HEADERS)
      if response.status_code == 200:
        logger.info(f"Successfully fetched {code} prices.")
        data = response.json()
        value = json.dumps(data).encode("utf-8")
        producer.produce(topic, key=code, value=value)
        logger.info(f"Produced {code} to {topic}.")
      else:
        return handle_status(code, response.status_code, logger)
    except requests.exceptions.ConnectionError as ce:
      logger.error(f"Connection error: {ce}")
    except requests.exceptions.RequestException as e:
      logger.error(f"Error fetching oil prices: {e}")

  producer.flush()
    