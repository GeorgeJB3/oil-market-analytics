from src.ingestion.http_utils import handle_status
import logging
import pytest


logger = logging.getLogger(__name__)


@pytest.mark.parametrize(
    "status_code, expected_message",
    [
        (400, "Bad Request (invalid parameters)."),
        (401, "Unauthorized (invalid API key)."),
        (429, "Too Many Requests (rate limit exceeded)."),
        (500, "Internal Server Error."),
        (404, "Unexpected error: 404."),
    ],
)
def test_handle_status_logs_message(caplog, status_code, expected_message):
    with caplog.at_level(logging.ERROR):
        handle_status("BRENT_CRUDE_USD", status_code, logger)
    assert expected_message in caplog.text


def test_handle_status_returns_none():
    result = handle_status("BRENT_CRUDE_USD", 400, logger)

    assert result is None
    