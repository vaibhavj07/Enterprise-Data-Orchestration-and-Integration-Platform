#!/bin/bash

# Define the base directory
BASE_DIR="/home/datashiptest/Enterprise-Data-Orchestration-and-Integration-Platform"

# Kafka and Spark configuration paths
KAFKA_CONFIG="${BASE_DIR}/config/kafka/kafka_config.json"
SPARK_CONFIG="${BASE_DIR}/config/spark_config.json"

# Set the PYTHONPATH to include the src directory
export PYTHONPATH="${BASE_DIR}/src:${PYTHONPATH}"

# Submit the Spark job
spark-submit --master yarn --deploy-mode cluster "${BASE_DIR}/src/consumer.py"
