import json
import os
from pyspark.sql import SparkSession
from google.cloud import storage
import urllib.request

#parse credentials.json
#returns dictionary
def fetch_credentials(bucket_name, file_path):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    credentials = json.loads(blob.download_as_string())

    return credentials

#for creating spark app for each source
def spark_session(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .enableHiveSupport() \
        .getOrCreate()

#extract sql
def test_extract_sql(spark, jdbc_url, query, driver):
    try:
        df = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .option("driver", driver) \
            .load()
        df.head()
    except Exception as e:
        print(f"Error reading data from JDBC source: {e}")
        raise



#test mongodb conn
def extract_mongo(spark, mongo_uri, table_name, driver):
    df = spark.read.format("jdbc") \
        .option("url",mongo_uri) \
        .option("dbtable", table_name) \
        .option("driver", driver) \
        .load()
    return df.head()


#


if __name__ == "__main__":

    #fetch json credentials
    bucket_name = "dataproc-staging-us-central1-2080396378-2dvlyk7i"
    file_path = 'credentials/credentials.json'

    credentials = fetch_credentials(bucket_name, file_path)

    #testing sql connection
    spark = spark_session("SQLDataIngestionTest")
    jdbc_url = credentials['spark']['jdbc-url']
    driver = credentials['sql_server']['driver']
    query = "(SELECT * FROM crm) AS query"

    test_extract_sql(spark, jdbc_url, driver, query)

    spark.stop()




    

    

