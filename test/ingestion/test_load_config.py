import pytest
from unittest.mock import patch
from src.ingestion.load_config import read_client


@pytest.mark.parametrize(
    "properties_text, expected",
    [
        pytest.param(
            "bootstrap.servers=host:9092\nsecurity.protocol=SASL_SSL",
            {"bootstrap.servers": "host:9092", "security.protocol": "SASL_SSL"},
            id="normal_lines",
        ),
        pytest.param(
            "# Kafka config\nbootstrap.servers=host:9092",
            {"bootstrap.servers": "host:9092"},
            id="comment_skipped",
        ),
        pytest.param(
            "bootstrap.servers=host:9092\n\nsecurity.protocol=SASL_SSL",
            {"bootstrap.servers": "host:9092", "security.protocol": "SASL_SSL"},
            id="blank_line_skipped",
        ),
        pytest.param(
            "sasl.password=abc=def",
            {"sasl.password": "abc=def"},
            id="value_containing_equals",
        ),
        pytest.param(
            "bootstrap.servers=  host:9092  ",
            {"bootstrap.servers": "host:9092"},
            id="value_whitespace_stripped",
        ),
        pytest.param(
            "",
            {},
            id="empty_input",
        ),
    ]
)


def test_read_client_parsing(properties_text, expected):
    with patch("src.ingestion.load_config.load_client", return_value=properties_text):
        assert read_client() == expected


def test_read_client_line_without_equals_raises():
    with patch("src.ingestion.load_config.load_client", return_value="no_equals_here"):
        with pytest.raises(ValueError):
            read_client()
