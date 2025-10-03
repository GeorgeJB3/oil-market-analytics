import pytest

from src.ingestion.oil_api_ingest import handle_status

def test_handle_status_logs_bad_request(caplog):
    handle_status("BRENT_CRUDE_USD", 400, logging.getLogger(__name__))
    assert "Bad Request (invalid parameters)." in caplog.text


def test_handle_status_logs_unauthorized(caplog):
    handle_status("BRENT_CRUDE_USD", 401, logging.getLogger(__name__))
    assert "Unauthorized (invalid API key)." in caplog.text


def test_handle_status_logs_rate_limit(caplog):
    handle_status("BRENT_CRUDE_USD", 429, logging.getLogger(__name__))
    assert "Too Many Requests (rate limit exceeded)." in caplog.text


def test_handle_status_logs_internal_server_error(caplog):
    handle_status("BRENT_CRUDE_USD", 500, logging.getLogger(__name__))
    assert "Internal Server Error." in caplog.text
