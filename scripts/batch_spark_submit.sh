#local test
spark-submit \
  --master local[*] \
  --deploy-mode client \
  --jars "/home/yasin/devtools/sqljdbc/mssql-jdbc-12.8.0.jre11.jar" \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
  /home/yasin/Enterprise-Data-Orchestration-and-Integration-Platform/src/ingestion/batch_data_ingestion_standalone.py \
  --app-name "BatchIngestionApp" \
  --hdfs-path "hdfs://dataship-cluster-m/landingzone1/" \
  --sql-jdbc-url "jdbc:sqlserver://34.46.196.232;databaseName=marketing;encrypt=true;trustServerCertificate=true;" \
  --sql-driver "com.microsoft.sqlserver.jdbc.SQLServerDriver" \
  --sql-username "sqlserver" \
  --sql-password "testing123" \
  --mongo-uri "mongodb+srv://datashiptest:testing%40123@source2-mongodb.e4alhqk.mongodb.net/?retryWrites=true&w=majority&appName=source2-mongodb" \
  --mongo-db-name "marketing_mongo"





  #checking sql connection using java for jre
  javac -cp /home/yasin/devtools/sqljdbc/mssql-jdbc-12.8.0.jre11.jar Enterprise-Data-Orchestration-and-Integration-Platform/src/tests/TestConnection.java
  java -cp /home/yasin/Enterprise-Data-Orchestration-and-Integration-Platform/src/tests:/home/yasin/devtools/sqljdbc/mssql-jdbc-12.8.0.jre11.jar TestConnection


  #delete outputs before reruns (if write mode is not overwrite)
  hdfs dfs -rm -r hdfs://dataship-cluster-m/landingzone1/crm && \
  hdfs dfs -rm -r hdfs://dataship-cluster-m/landingzone1/transactions && \
  hdfs dfs -rm -r hdfs://dataship-cluster-m/landingzone1/attribution && \
  hdfs dfs -rm -r hdfs://dataship-cluster-m/landingzone1/campaign_metadata && \
  hdfs dfs -rm -r hdfs://dataship-cluster-m/landingzone1/customer_profiles && \
  hdfs dfs -rm -r hdfs://dataship-cluster-m/landingzone1/product_catalogs


  #testing
  python3 Enterprise-Data-Orchestration-and-Integration-Platform/src/tests/crm_test.py \
    --app-name "BatchIngestionApp" \
    --hdfs-path "hdfs://dataship-cluster-m/landingzone1/" \
    --sql-jdbc-url "jdbc:sqlserver://your-sql-server-url;databaseName=your_db" \
    --sql-driver "com.microsoft.sqlserver.jdbc.SQLServerDriver" \
    --sql-username "sqlserver" \
    --sql-password "testing123"


  #mongo_conn_test_1
  spark-submit \
  --master local[*] \
  --deploy-mode client \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
  /home/yasin/Enterprise-Data-Orchestration-and-Integration-Platform/src/tests/mongo_conn_test.py

  #mongo_conn_test_2
  spark-submit \
  --master local[*] \
  --deploy-mode client \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
  /home/yasin/Enterprise-Data-Orchestration-and-Integration-Platform/src/tests/mongo_conn_test.py \
  --app-name "MongoConnectionTest" \
  --mongo-uri "mongodb+srv://datashiptest:testing%40123@source2-mongodb.e4alhqk.mongodb.net/?retryWrites=true&w=majority&appName=source2-mongodb" \
  --mongo-db-name "marketing_mongo"


  #path to hdfs
  hadoop fs -ls hdfs://dataship-cluster-m/landingzone1/


#dataproc
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --jars "gs://dataproc-staging-us-central1-2080396378-2dvlyk7i/dependencies/jars/mssql-jdbc-12.8.0.jre8.jar" \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.1.1 \
  /home/yasin/Enterprise-Data-Orchestration-and-Integration-Platform/src/ingestion/batch_data_ingestion_standalone.py \
  --app-name "BatchIngestionApp" \
  --hdfs-path "hdfs://dataship-cluster-m/landingzone1/" \
  --sql-jdbc-url "jdbc:sqlserver://10.123.208.3;databaseName=marketing;encrypt=true;trustServerCertificate=true;" \
  --sql-driver "com.microsoft.sqlserver.jdbc.SQLServerDriver" \
  --sql-username "sqlserver" \
  --sql-password "testing123" \
  --mongo-uri "mongodb+srv://datashiptest:testing%40123@source2-mongodb.e4alhqk.mongodb.net/?retryWrites=true&w=majority&appName=source2-mongodb" \
  --mongo-db-name "marketing_mongo"


