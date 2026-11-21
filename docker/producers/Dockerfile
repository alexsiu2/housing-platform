# News producers (streaming/*_producer.py): small Python services that poll
# sources and publish Avro events to Kafka. Code is mounted, not copied.
FROM python:3.12-slim

COPY requirements-producers.txt /tmp/requirements-producers.txt
RUN pip install --no-cache-dir -r /tmp/requirements-producers.txt \
    && useradd --create-home --uid 10001 producer
USER producer
WORKDIR /app
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
