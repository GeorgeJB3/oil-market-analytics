import yaml
from pathlib import Path

def load_config(file="config/config.yaml"):
    """
    Load API configuration from a YAML file.

    Resolves the repository root, reads the specified YAML config file,
    and parses its contents into a Python dictionary.

    Args:
        file (str): Relative path to the YAML configuration file.
            Defaults to "config/config.yaml".

    Returns:
        dict: Parsed configuration values from the YAML file.
    """
    repo_root = Path(__file__).resolve().parents[2]
    config_path = repo_root / file
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


def load_client(file="config/client.properties"):
    """
    Load Kafka client configuration from a properties file.

    Resolves the repository root, reads the specified client properties file,
    and returns its raw contents as a string.

    Args:
        file (str): Relative path to the Kafka client properties file.
            Defaults to "config/client.properties".

    Returns:
        str: Raw contents of the client properties file.
    """
    repo_root = Path(__file__).resolve().parents[2]
    config_path = repo_root / file
    with open(config_path, "r") as f:
        config = f.read()
    return config


def read_client():
  """
    Parse Kafka client configuration into a dictionary.

    Reads the client properties file using `load_client`, splits each line
    into key/value pairs, and ignores comments or empty lines. Produces a
    dictionary suitable for initialising Kafka clients.

    Returns:
        dict: Kafka client configuration parameters.
    """
  config = {}
  client = load_client()
  # with open("client.properties") as client:
  for line in client.splitlines():
    line = line.strip()
    if len(line) != 0 and line[0] != "#":
      parameter, value = line.strip().split('=', 1)
      config[parameter] = value.strip()
  return config
