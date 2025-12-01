# Use official Airflow image as base
FROM apache/airflow:2.8.0-python3.11

USER root

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
