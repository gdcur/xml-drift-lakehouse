FROM apache/airflow:2.10.4-python3.11

# Switch to root to install system deps
USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user for pip installs
USER airflow

# Install project dependencies
COPY docker/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
