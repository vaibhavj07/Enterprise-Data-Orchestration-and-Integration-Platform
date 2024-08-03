#!/bin/bash

# Set Kafka directory
KAFKA_DIR=/home/datashiptest/kafka_2.12-3.7.1

# Function to start Zookeeper
start_zookeeper() {
    echo "Starting Zookeeper..."
    $KAFKA_DIR/bin/zookeeper-server-start.sh -daemon $KAFKA_DIR/config/zookeeper.properties
    if [ $? -eq 0 ]; then
        echo "Zookeeper started successfully."
    else
        echo "Failed to start Zookeeper."
        exit 1
    fi
}

# Function to start Kafka
start_kafka() {
    echo "Starting Kafka..."
    $KAFKA_DIR/bin/kafka-server-start.sh -daemon $KAFKA_DIR/config/server.properties
    if [ $? -eq 0 ]; then
        echo "Kafka started successfully."
    else
        echo "Failed to start Kafka."
        exit 1
    fi
}

# Start Zookeeper
start_zookeeper

# Wait for Zookeeper to start up properly
echo "Waiting for Zookeeper to initialize..."
sleep 10

# Start Kafka
start_kafka

echo "Kafka and Zookeeper have been started in the background."

