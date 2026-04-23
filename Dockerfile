FROM apache/airflow:3.2.0

USER root

# Install system packages if needed
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install additional Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
