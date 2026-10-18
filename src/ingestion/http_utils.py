import logging


logger = logging.getLogger(__name__)


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