import requests
import logging
import json

from confluent_kafka import Producer

from src.ingestion.load_config import load_config, read_client


def handle_status(code, status_code, logger): 
  """
  Handle different status codes and print corresponding messages.
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
  Fetch WTI crude oil, Brent crude oil, and natural gas prices from the OilPrice API.
  Then sends the data to Kafka.
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
    except requests.exceptions.RequestException as e:
      logger.error(f"Error fetching oil prices: {e}")

  producer.flush()
    