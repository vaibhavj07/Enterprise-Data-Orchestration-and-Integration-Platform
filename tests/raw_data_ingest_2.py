import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_date
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, ArrayType

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("KafkaRawIngestionTest") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2") \
    .enableHiveSupport() \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
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
    .option("kafka.bootstrap.servers", "10.128.0.7:9092") \
    .option("subscribe", "website_events") \
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

# Function to log data in each batch
def log_data_in_batches(batch_df, batch_id):
    logger.info(f"Batch ID: {batch_id}")
    batch_df.show(truncate=False)

# Use foreachBatch to log each batch of data
log_query = df.writeStream \
    .foreachBatch(log_data_in_batches) \
    .start()

# Create an external Hive table if not exists
spark.sql("""
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
    LOCATION 'hdfs://dataship-cluster-m/landingzone1/websiteevents'
""")
logger.info("External Hive table created.")

# Write raw data to HDFS partitioned by date
hdfs_query = df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "hdfs://dataship-cluster-m/landingzone1/websiteevents") \
    .option("checkpointLocation", "hdfs://dataship-cluster-m/tmp/checkpoints/hdfs") \
    .partitionBy("date") \
    .start()

logger.info("Writing data to HDFS.")

log_query.awaitTermination()
hdfs_query.awaitTermination()
