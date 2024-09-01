import logging
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def batch_ingestion():
    # Logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Initialize argument parser
    parser = argparse.ArgumentParser(description="SQL Server and Mongo to HDFS")

    parser.add_argument('--app-name', required=True, help='spark app name')
    parser.add_argument('--hdfs-path', required=True, help='HDFS storage path')

    parser.add_argument('--sql-jdbc-url', required=True, help='JDBC url for SQL Server')
    parser.add_argument('--sql-driver', required=True, help='JDBC driver class for SQL Server')
    parser.add_argument('--sql-username', required=True, help='Username for SQL Server')
    parser.add_argument('--sql-password', required=True, help='Password for SQL Server')

    parser.add_argument('--mongo-uri', required=True, help='MongoDB URI for connection')
    parser.add_argument('--mongo-db-name', required=True, help='MongoDB database name')

    args = parser.parse_args()

    # Initialize Spark session


    spark = SparkSession.builder \
    .appName(args.app_name) \
    .config("spark.jars", "mssql-jdbc-12.8.0.jre11.jar") \
    .config("spark.mongodb.read.connection.uri", args.mongo_uri) \
    .config("spark.mongodb.connection.timeoutMS", "60000") \
    .config("spark.mongodb.connection.debug", "true") \
    .enableHiveSupport() \
    .getOrCreate()

    #set logging level here
    spark.sparkContext.setLogLevel("ERROR")

    logger.info("Spark session initialized.")
    logger.info(f"Using Mongo URI: {args.mongo_uri}")


    ### SQL Processing ###

    # Process CRM table
    crm_query = "SELECT * FROM crm"

    logger.info("Extracting SQL data for CRM...")
    crm_df = spark.read.format("jdbc") \
        .option("url", args.sql_jdbc_url) \
        .option("query", crm_query) \
        .option("driver", args.sql_driver) \
        .option("user", args.sql_username) \
        .option("password", args.sql_password) \
        .load()

    crm_df.write.partitionBy("SignupDate").mode("overwrite").parquet(f"{args.hdfs_path}/sqlserver/crm")
    logger.info("Data written to HDFS for SQL table: crm")

    crm_count = crm_df.count()
    logging.info(f"Records to write: {crm_count}")

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS crm (
            CustomerID INT,
            CustomerName STRING,
            Email STRING,
            Phone STRING,
            SignupDate TIMESTAMP
        )
        STORED AS PARQUET
        LOCATION '{args.hdfs_path}/crm';
    """)
    logger.info("Hive external table created for SQL table: crm")

    # Process Transactions table
    transactions_query = "SELECT * FROM transactions"

    logger.info("Extracting SQL data for Transactions...")
    transactions_df = spark.read.format("jdbc") \
        .option("url", args.sql_jdbc_url) \
        .option("query", transactions_query) \
        .option("driver", args.sql_driver) \
        .option("user", args.sql_username) \
        .option("password", args.sql_password) \
        .load()

    transactions_count = transactions_df.count()
    logging.info(f"Records to write: {transactions_count}")

    transactions_df.write.partitionBy("TransactionDate").mode("overwrite").parquet(f"{args.hdfs_path}/sqlserver/transactions")
    logger.info("Data written to HDFS for SQL table: transactions")

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS transactions (
            TransactionID INT,
            CustomerID INT,
            ProductID INT,
            Quantity INT,
            TransactionDate TIMESTAMP,
            TransactionAmount DOUBLE
        )
        STORED AS PARQUET
        LOCATION '{args.hdfs_path}/transactions';
    """)
    logger.info("Hive external table created for SQL table: transactions")

    # Process Attribution table
    attribution_query = "SELECT * FROM attribution"

    logger.info("Extracting SQL data for Attribution...")
    attribution_df = spark.read.format("jdbc") \
        .option("url", args.sql_jdbc_url) \
        .option("query", attribution_query) \
        .option("driver", args.sql_driver) \
        .option("user", args.sql_username) \
        .option("password", args.sql_password) \
        .load()

    attribution_count = attribution_df.count()
    logging.info(f"Records to write: {attribution_count}")

    attribution_df.write.partitionBy("AttributionDate").mode("overwrite").parquet(f"{args.hdfs_path}/sqlserver/attribution")
    logger.info("Data written to HDFS for SQL table: attribution")

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS attribution (
            AttributionID INT,
            CustomerID INT,
            CampaignID INT,
            AttributionDate TIMESTAMP,
            AttributionValue DOUBLE
        )
        STORED AS PARQUET
        LOCATION '{args.hdfs_path}/attribution';
    """)
    logger.info("Hive external table created for SQL table: attribution")

    ### MongoDB Processing ###

    # Process campaign_metadata collection
    campaign_metadata_df = spark.read.format("mongodb") \
        .schema(StructType([
            StructField("_id", StringType(), False),
            StructField("campaign_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("start_date", StringType(), True),
            StructField("end_date", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("budget", DoubleType(), True)
        ])) \
        .option("database", args.mongo_db_name) \
        .option("collection", "campaign_metadata") \
        .load()

    campaign_metadata_count = campaign_metadata_df.count()
    logging.info(f"Records to write: {campaign_metadata_count}")

    campaign_metadata_df = campaign_metadata_df.withColumn("start_date", to_timestamp(col("start_date"), "yyyy-MM-dd HH:mm:ss"))
    campaign_metadata_df = campaign_metadata_df.withColumn("end_date", to_timestamp(col("end_date"), "yyyy-MM-dd HH:mm:ss"))
    campaign_metadata_df = campaign_metadata_df.withColumn("date", to_date(col("start_date")))
    campaign_metadata_df = campaign_metadata_df.filter(col("date").isNotNull())

    campaign_metadata_df.write.partitionBy("start_date").mode("overwrite").parquet(f"{args.hdfs_path}/mongo/campaign_metadata")
    logger.info("Data written to HDFS for MongoDB collection: campaign_metadata")

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS campaign_metadata (
            _id STRING,
            campaign_id INT,
            name STRING,
            start_date TIMESTAMP,
            end_date TIMESTAMP,
            channel STRING,
            budget DOUBLE
        )
        STORED AS PARQUET
        LOCATION '{args.hdfs_path}/mongo/campaign_metadata';
    """)
    logger.info("Hive external table created for MongoDB collection: campaign_metadata")

    # Process customer_profiles collection
    customer_profiles_df = spark.read.format("mongodb") \
        .schema(StructType([
            StructField("_id", StringType(), False),
            StructField("customer_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("email", StringType(), True),
            StructField("address", StructType([
                StructField("street", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state", StringType(), True),
                StructField("zip", StringType(), True)
            ]), True)
        ])) \
        .option("database", args.mongo_db_name) \
        .option("collection", "customer_profiles") \
        .load()
    
    customer_profiles_count = customer_profiles_df.count()
    logging.info(f"Records to write: {customer_profiles_count}")
    
    customer_profiles_df.write.mode("overwrite").parquet(f"{args.hdfs_path}/mongo/customer_profiles")
    logger.info("Data written to HDFS for MongoDB collection: customer_profiles")

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS customer_profiles (
            _id STRING,
            customer_id INT,
            name STRING,
            age INT,
            gender STRING,
            email STRING,
            street STRING,
            city STRING,
            state STRING,
            zip STRING
        )
        STORED AS PARQUET
        LOCATION '{args.hdfs_path}/customer_profiles';
    """)
    logger.info("Hive external table created for MongoDB collection: customer_profiles")

    # Process product_catalogs collection
    product_catalogs_df = spark.read.format("mongodb") \
        .schema(StructType([
            StructField("_id", StringType(), False),
            StructField("product_id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("description", StringType(), True)
        ])) \
        .option("database", args.mongo_db_name) \
        .option("collection", "product_catalogs") \
        .load()

    product_catalogs_count = customer_profiles_df.count()
    logging.info(f"Records to write: {customer_profiles_count}")
    
    product_catalogs_df.write.partitionBy("category").mode("overwrite").parquet(f"{args.hdfs_path}/mongo/product_catalogs")
    logger.info("Data written to HDFS for MongoDB collection: product_catalogs")

    spark.sql(f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS product_catalogs (
            _id STRING,
            product_id INT,
            name STRING,
            category STRING,
            price DOUBLE,
            description STRING
        )
        STORED AS PARQUET
        LOCATION '{args.hdfs_path}/product_catalogs';
    """)
    logger.info("Hive external table created for MongoDB collection: product_catalogs")

    # Stop Spark session
    logger.info("Stopping Spark session.")
    spark.stop()

if __name__ == "__main__":
    batch_ingestion()
