from pyspark.sql import SparkSession

def read_from_kafka(spark: SparkSession, kafka_config: dict):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", ",".join(kafka_config['bootstrap_servers'])) \
        .option("subscribe", kafka_config['subscribe_topic']) \
        .load()
    return df
