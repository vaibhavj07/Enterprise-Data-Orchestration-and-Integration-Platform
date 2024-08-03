from pyspark.sql import SparkSession
from config import load_spark_config
from logging_utils import setup_logging

# Set up logging
logger = setup_logging()

def create_spark_session(app_name, use_case="default"):
    try:
        spark_config = load_spark_config(use_case)
        spark = SparkSession.builder \
            .appName(app_name) \
            .master("yarn") \
            .enableHiveSupport()
        
        for key, value in spark_config.items():
            if key != "hdfs_path":
                spark = spark.config(key, value)
        
        spark = spark.getOrCreate()
        logger.info("Spark session initialized.")
        return spark, spark_config["hdfs_path"]
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        return None, None
