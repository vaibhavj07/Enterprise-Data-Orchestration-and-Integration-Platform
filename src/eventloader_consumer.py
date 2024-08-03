import logging
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, ArrayType
from config import load_config
from kafka_reader import read_from_kafka
from logging_utils import setup_logging
from spark_session_builder import create_spark_session

# Set up logging
logger = setup_logging()

# Load Kafka configuration
kafka_config = load_config('/home/datashiptest/marketing-project/config/kafka/kafka_config.json')

# Create Spark session
spark, landing_zone_path = create_spark_session(app_name="KafkaRawIngestion", use_case="websiteevents")
if not spark:
    raise Exception("Failed to create Spark session")

# Define schema for incoming data
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("referrer", StringType(), True),
    StructField("interaction_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("interaction_type", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("conversion_id", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("conversion_value", FloatType(), True),
    StructField("signup_id", StringType(), True),
    StructField("email", StringType(), True),
    StructField("signup_source", StringType(), True),
    StructField("start_time", StringType(), True),
    StructField("end_time", StringType(), True),
    StructField("duration", FloatType(), True),
    StructField("pages_viewed", StringType(), True),
    StructField("actions", ArrayType(StringType()), True)
])

logger.info("Schema defined.")

# Read data from Kafka
df = read_from_kafka(spark, kafka_config)

logger.info("Data read from Kafka.")

# Convert value column to string
df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON data and apply schema
df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Convert timestamp column to TimestampType
df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Extract date from timestamp for partitioning
df = df.withColumn("date", to_date(col("timestamp")))

# Create an external Hive table if not exists
def create_hive_table(spark, landing_zone_path):
    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS prd.website_events (
            event_id STRING,
            user_id STRING,
            page_url STRING,
            timestamp TIMESTAMP,
            event_type STRING,
            referrer STRING,
            interaction_id STRING,
            product_id STRING,
            interaction_type STRING,
            session_id STRING,
            conversion_id STRING,
            campaign_id STRING,
            conversion_value FLOAT,
            signup_id STRING,
            email STRING,
            signup_source STRING,
            start_time STRING,
            end_time STRING,
            duration FLOAT,
            pages_viewed STRING,
            actions ARRAY<STRING>
        ) PARTITIONED BY (date STRING)
        STORED AS PARQUET
        LOCATION '{landing_zone_path}'
    """)

create_hive_table(spark, landing_zone_path)

logger.info("External Hive table created.")

# Write raw data to HDFS partitioned by date
hdfs_query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", landing_zone_path) \
    .option("checkpointLocation", spark.conf.get("spark.sql.streaming.checkpointLocation")) \
    .partitionBy("date") \
    .start()

logger.info("Writing data to HDFS.")

hdfs_query.awaitTermination()
