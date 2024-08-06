import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, ArrayType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Parse arguments
parser = argparse.ArgumentParser(description="Kafka to HDFS Spark Streaming")
parser.add_argument('--checkpoint-location', required=True, help='HDFS checkpoint location')
parser.add_argument('--hdfs-path', required=True, help='HDFS path for data storage')
parser.add_argument('--bootstrap-servers', required=True, help='Kafka bootstrap servers')
parser.add_argument('--subscribe-topic', required=True, help='Kafka topic to subscribe to')
args = parser.parse_args()

logger.info("Checkpoint Location: %s", args.checkpoint_location)
logger.info("Bootstrap Servers: %s", args.bootstrap_servers)
logger.info("HDFS Path: %s", args.hdfs_path)
logger.info("Subscribe Topic: %s", args.subscribe_topic)

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("KafkaRawIngestionTest") \
    .master("yarn") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .enableHiveSupport() \
    .getOrCreate()

logger.info("Spark session initialized.")

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
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", args.bootstrap_servers) \
    .option("subscribe", args.subscribe_topic) \
    .option("failOnDataLoss", "false") \
    .load()

logger.info("Data read from Kafka.")

# Convert value column to string
df = df.selectExpr("CAST(value AS STRING)")

# Parse JSON data and apply schema
df = df.select(from_json(col("value"), schema).alias("data")).select("data.*")

# Convert timestamp column to TimestampType
df = df.withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Extract date from timestamp for partitioning
df = df.withColumn("date", to_date(col("timestamp")))

# Filter out rows with null date to avoid default partition
df = df.filter(col("date").isNotNull())

# Add debugging output to show some sample data
df_sample_query = df.writeStream \
    .format("console") \
    .option("truncate", False) \
    .start()

# Define a custom ForeachWriter to log each row
class LogRowWriter:
    def open(self, partition_id, epoch_id):
        return True

    def process(self, row):
        logger.info(f"Processed row: {row}")

    def close(self, error):
        if error:
            logger.error(f"Error: {error}")

# Use the custom ForeachWriter to log each row
log_query = df.writeStream \
    .foreach(LogRowWriter()) \
    .start()

# Create an external Hive table if not exists
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
    LOCATION '{args.hdfs_path}'
""")
logger.info("External Hive table created.")

# Write raw data to HDFS partitioned by date
hdfs_query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", args.hdfs_path) \
    .option("checkpointLocation", args.checkpoint_location) \
    .partitionBy("date") \
    .start()

logger.info("Writing data to HDFS.")

# Wait for the queries to terminate
try:
    log_query.awaitTermination()
    df_sample_query.awaitTermination()
    hdfs_query.awaitTermination()
except Exception as e:
    logger.error("Error during streaming: %s", str(e))
    raise
