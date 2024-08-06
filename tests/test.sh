#!/bin/bash

# Paths to the JSON files
spark_json_file="/home/datashiptest/Enterprise-Data-Orchestration-and-Integration-Platform/config/spark_config.json"
kafka_json_file="/home/datashiptest/Enterprise-Data-Orchestration-and-Integration-Platform/config/kafka/kafka_config.json"

# Read the Spark JSON file content
spark_json_content=$(cat "$spark_json_file")

# Extract the 'websiteevents' object content
websiteevents_content=$(echo "$spark_json_content" | awk '/"websiteevents": \{/,/\}/')

# Extract the values for 'spark.sql.streaming.checkpointLocation' and 'hdfs_path'
checkpoint_location=$(echo "$websiteevents_content" | awk -F'"spark.sql.streaming.checkpointLocation": "' '{print $2}' | awk -F'"' '{print $1}')
hdfs_path=$(echo "$websiteevents_content" | awk -F'"hdfs_path": "' '{print $2}' | awk -F'"' '{print $1}')

# Trim any leading or trailing whitespace
checkpoint_location=$(echo "$checkpoint_location" | xargs)
hdfs_path=$(echo "$hdfs_path" | xargs)

# Read the Kafka JSON file content
kafka_json_content=$(cat "$kafka_json_file")

# Extract the Kafka credentials
bootstrap_servers=$(echo "$kafka_json_content" | awk -F'"bootstrap_servers": ' '{print $2}' | awk -F']' '{print $1}' | tr -d '[]' | tr -d '"')
subscribe_topic=$(echo "$kafka_json_content" | awk -F'"subscribe_topic": "' '{print $2}' | awk -F'"' '{print $1}')

# Trim any leading or trailing whitespace
bootstrap_servers=$(echo "$bootstrap_servers" | xargs)
subscribe_topic=$(echo "$subscribe_topic" | xargs)

# Output the extracted values
echo "Checkpoint Location:$checkpoint_location"
echo "HDFS Path:$hdfs_path"
echo "Bootstrap Servers:$bootstrap_servers"
echo "Subscribe Topic:$subscribe_topic"