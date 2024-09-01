import json
import os
from pyspark.sql import SparkSession

def fetch_credentials(file_path):
    with open(file_path, 'r') as file:
        credentials = json.load(file)
    return credentials

#for creating spark app for sql db
def spark_session_sql(app_name, jdbc_jar_path):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", jdbc_jar_path) \
        .enableHiveSupport() \
        .getOrCreate()

#for extracting from sql db
def extract_sql(spark, jdbc_url, query, driver):
    try:
        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("driver", driver) \
            .option("query", query) \
            .load()
        return df
    except Exception as e:
        print(f"Error reading data from JDBC source: {e}")
        raise


if __name__ == "__main__":
    file_path = "/home/yasin/Enterprise-Data-Orchestration-and-Integration-Platform/config/credentials_yasin.json"  # Replace with your actual JSON file path
    credentials = fetch_credentials(file_path)
    print(credentials)

