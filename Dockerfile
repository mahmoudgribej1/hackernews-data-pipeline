FROM apache/airflow:2.7.1

# Switch to root to install Java
USER root

# Install OpenJDK 11 (required for PySpark)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Switch back to airflow user
USER airflow

# Install Python packages
RUN pip install pandas pyspark pyarrow sqlalchemy psycopg2-binary boto3