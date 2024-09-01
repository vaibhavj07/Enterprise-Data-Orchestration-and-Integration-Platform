import os
import sys
import argparse
import logging
from utils.ingestion_utils import fetch_credentials, extract_sql, spark_session_sql
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType, ArrayType

#logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#parse args for command line
parser = argparse.ArgumentParser(description="SQL Server to HDFS")

parser.add_argument('--jar-path', required=True, help='JDBC JAR file for sql conn')

#enable config file
file_path = "/home/yasin/Enterprise-Data-Orchestration-and-Integration-Platform/config/credentials_yasin.json"
credentials = fetch_credentials(file_path)

logger.info("Credentials fetched from: %s", file_path)

#EXTRACT STARTS HERE
#sql config
app_name = "SQLDataIngestion"
jdbc_jar_path = credentials["spark"]["jdbc-path"]

spark = spark_session_sql(app_name, jdbc_jar_path)

logger.info("Spark session initialized.")

#specify params for extracting sql data from server
jdbc_url = credentials['spark']['jdbc-url']
query = "(SELECT TOP 5 * FROM crm) AS query"
driver = credentials['sql_server']['driver']


extract_sql(spark, jdbc_url, query, driver)

spark.stop()



