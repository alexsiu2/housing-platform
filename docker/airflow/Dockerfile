FROM apache/airflow:3.4.0-python3.12

# Project dependencies needed inside Airflow tasks. Re-pinning apache-airflow
# stops pip from upgrading/downgrading Airflow while resolving these.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==3.4.0" -r /tmp/requirements-airflow.txt
