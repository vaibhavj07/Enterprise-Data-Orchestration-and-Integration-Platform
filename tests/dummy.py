import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql import Row
import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark session for YARN
spark = SparkSession.builder \
    .appName("SimpleDataIngestion") \
    .master("yarn") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "2") \
    .getOrCreate()

logger.info("Spark session initialized.")

# Define schema for the data
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("conversion_value", FloatType(), True),
    StructField("date", StringType(), True)
])

# Sample data with all fields matching the schema
data = [
    {"user_id": "user_14", "product_id": "product_33", "conversion_value": 422.55, "date": "2024-08-05"},
    {"user_id": "user_25", "product_id": "product_15", "conversion_value": 118.65, "date": "2024-08-05"},
    {"user_id": "user_72", "product_id": "product_41", "conversion_value": 220.75, "date": "2024-08-05"},
    {"user_id": "user_53", "product_id": "product_15", "conversion_value": 318.65, "date": "2024-08-05"},
    {"user_id": "user_90", "product_id": "product_29", "conversion_value": 500.00, "date": "2024-08-05"}
]

# Create DataFrame
rows = [Row(**item) for item in data]
df = spark.createDataFrame(rows, schema)

# Write data to HDFS partitioned by date
df.write \
    .format("parquet") \
    .mode("append") \
    .option("path", "hdfs://dataship-cluster-m:8051/landingzone1/websiteevents") \
    .partitionBy("date") \
    .save()

logger.info("Data written to HDFS.")
