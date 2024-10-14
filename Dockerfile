# Base image with Python
FROM python:3.9-slim

# Update and install necessary packages
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    bash \
    netcat-openbsd \
    libsasl2-dev \
    libpq-dev \
    gcc \
    unixodbc-dev && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set environment variables for Java and Spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Set working directory
WORKDIR /app

# Copy the project files into the container
COPY . /app

# Install Python dependencies
RUN pip install --upgrade pip && \
   pip install --no-cache-dir -r requirements.txt

# Expose necessary ports (Airflow/Spark UI)
EXPOSE 8080
EXPOSE 4040

# Define entrypoint (this could be your entry script, Airflow DAGs, etc.)
CMD ["bash", "entrypoint.sh"]
