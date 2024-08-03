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

def load_spark_config(use_case="default"):
    try:
        config_path = '/home/datashiptest/marketing-project/config/spark_config.json'
        with open(config_path, 'r') as f:
            config = json.load(f)
        if use_case in config:
            logger.info(f"Loaded Spark config for use case '{use_case}'")
            return config[use_case]
        else:
            raise ValueError(f"Configuration for use case '{use_case}' not found in spark_config.json")
    except Exception as e:
        logger.error(f"Error loading Spark config: {e}")
        return {}

# Example usage
kafka_config = load_config('../config/kafka/kafka_config.json')
kafka_secrets = load_config('../config/kafka/kafka_secrets.json')
spark_config = load_spark_config(use_case="websiteevents")
