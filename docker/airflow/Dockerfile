FROM apache/airflow:3.4.0-python3.12

# Project dependencies needed inside Airflow tasks. Re-pinning apache-airflow
# stops pip from upgrading/downgrading Airflow while resolving these.
COPY requirements-airflow.txt /tmp/requirements-airflow.txt
RUN pip install --no-cache-dir "apache-airflow==3.4.0" -r /tmp/requirements-airflow.txt

# dbt in an isolated virtualenv, called by dags/dbt_build.py as /opt/dbt-venv/bin/dbt.
USER root
RUN mkdir -p /opt/dbt-venv && chown airflow:0 /opt/dbt-venv
USER airflow
COPY requirements-dbt.txt /tmp/requirements-dbt.txt
RUN python -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/requirements-dbt.txt
