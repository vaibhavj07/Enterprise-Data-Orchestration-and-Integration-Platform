
import json
import os
from logging_utils import setup_logging

# Set up logging
logger = setup_logging()

def load_config(config_path):
    try:
        with open(config_path, 'r') as file:
            config = json.load(file)
        logger.info(f"Loaded config from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading config from {config_path}: {e}")
        return {}

# Example usage
kafka_config = load_config('../config/kafka/kafka_config.json')
kafka_secrets = load_config('../config/kafka/kafka_secrets.json')

