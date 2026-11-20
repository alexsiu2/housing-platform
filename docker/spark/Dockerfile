# Spark 4.2.0 (Java 21, same as the Airflow driver) with Delta Lake 4.4.1 + S3A.
FROM apache/spark:4.2.0-scala2.13-java21-python3-ubuntu

USER root
COPY docker/spark/jars.txt docker/spark/fetch_jars.sh /tmp/jars/
RUN /tmp/jars/fetch_jars.sh /opt/spark/jars && rm -rf /tmp/jars
USER spark
