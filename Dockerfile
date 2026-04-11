# Use official Airflow image as base
FROM apache/airflow:2.8.0-python3.11

USER root

# Install Java and dependencies for Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Download and install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

RUN curl -fsSL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | \
    tar -xz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    chown -R airflow:root ${SPARK_HOME}

# Create data directories
RUN mkdir -p /opt/airflow/data/bronze /opt/airflow/data/silver /opt/airflow/data/gold && \
    chown -R airflow:root /opt/airflow/data

# Copy requirements and fix permissions
COPY requirements.txt /requirements.txt
RUN chown airflow:root /requirements.txt

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Set Python path to include utils directory
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

WORKDIR /opt/airflow
