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

# --- Spark client (Phase 5): SparkSubmitOperator runs the Spark driver here in
# client mode against the `spark` compose profile. Needs Java 21 (the base is
# Debian 13; same as the Spark image), the exact PySpark
# version of the cluster (4.2.0), and the same Delta + S3A jars as the workers.
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "/usr/lib/jvm/java-21-openjdk-$(dpkg --print-architecture)" /usr/lib/jvm/rpof-java
COPY docker/spark/jars.txt docker/spark/fetch_jars.sh /tmp/jars/
RUN /tmp/jars/fetch_jars.sh /opt/rpof-spark-jars && rm -rf /tmp/jars
ENV JAVA_HOME=/usr/lib/jvm/rpof-java
USER airflow
COPY requirements-spark-client.txt /tmp/requirements-spark-client.txt
RUN pip install --no-cache-dir "apache-airflow==3.4.0" -r /tmp/requirements-spark-client.txt
