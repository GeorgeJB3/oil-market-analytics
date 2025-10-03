import yaml
from pathlib import Path

def load_config(file="config/config.yaml"):
    '''load api credentials'''
    repo_root = Path(__file__).resolve().parents[2]
    config_path = repo_root / file
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


def load_client(file="config/client.properties"):
    '''load kafka configuration'''
    repo_root = Path(__file__).resolve().parents[2]
    config_path = repo_root / file
    with open(config_path, "r") as f:
        config = f.read()
    return config


def read_client():
  '''reads the kafka client configuration and returns it as a dict'''
  config = {}
  client = load_client()
  # with open("client.properties") as client:
  for line in client.splitlines():
    line = line.strip()
    if len(line) != 0 and line[0] != "#":
      parameter, value = line.strip().split('=', 1)
      config[parameter] = value.strip()
  return config