# Minimal custom container for dbt Python models on Dataproc Serverless.
FROM python:3.10-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends procps tini \
    && rm -rf /var/lib/apt/lists/*

COPY requirements-dataproc.txt /tmp/requirements-dataproc.txt
RUN python -m pip install --no-cache-dir --upgrade -r /tmp/requirements-dataproc.txt

ENV PYSPARK_PYTHON=/usr/local/bin/python

# Dataproc requires UID/GID 1099 and a home directory for the spark user.
RUN groupadd -g 1099 spark && useradd -u 1099 -g 1099 -d /home/spark -m spark
USER spark
