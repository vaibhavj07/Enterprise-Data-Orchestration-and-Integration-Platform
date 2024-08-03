#!/bin/bash

# Navigate to the project src directory
cd "$(dirname "$0")/../src"

# Activate the Python virtual environment if applicable
# source ../venv/bin/activate

# Run the Kafka producer script
python kafka_producer.py
