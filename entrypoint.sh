#!/bin/bash

# Function to start Airflow webserver and scheduler
start_airflow() {
    echo "Starting Airflow..."
    airflow db init               # Initialize Airflow database
    airflow webserver -p 8080 &   # Start Airflow webserver
    airflow scheduler &           # Start Airflow scheduler
    echo "Airflow started"
}

# Find and kill the Airflow web server and scheduler processes
stop_airflow() {
    echo "Stopping Airflow..."
    pgrep -f "airflow webserver" | xargs kill
    pgrep -f "airflow scheduler" | xargs kill
    echo "Airflow stopped"
}

# Function to run Spark job
run_spark_job() {
    echo "Running Spark job..."
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
}

# Function to run a shell script
run_shell_script() {
    echo "Running shell script..."
    bash /app/scripts/some_shell_script.sh
    echo "Shell script executed"
}

# Check the argument provided when starting the container
if [ "$1" = "start_airflow" ]; then
    start_airflow
    tail -f /dev/null  # Keeps the container running for Airflow
elif [ "$1" = "stop_airflow" ]; then
    stop_airflow
elif [ "$1" = "spark" ]; then
    run_spark_job
elif [ "$1" = "shell" ]; then
    run_shell_script
else
    echo "No valid option provided. Available options are: airflow, spark, shell"
    echo "Example: docker run -it <image_name> airflow"
fi
