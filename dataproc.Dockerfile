# Minimal custom container for dbt Python models on Dataproc Serverless.
FROM python:3.10-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends procps tini \
    && rm -rf /var/lib/apt/lists/*

# Resolved from PyPI by the build workflow, so new releases need no file change.
ARG PACKAGE_VERSION
COPY requirements-dataproc.txt /tmp/requirements-dataproc.txt
RUN test -n "$PACKAGE_VERSION" \
    && python -m pip install --no-cache-dir --upgrade -r /tmp/requirements-dataproc.txt \
    "rio-rac-bus-subsidy==${PACKAGE_VERSION}"

ENV PYSPARK_PYTHON=/usr/local/bin/python

# Dataproc requires UID/GID 1099 and a home directory for the spark user.
RUN groupadd -g 1099 spark && useradd -u 1099 -g 1099 -d /home/spark -m spark
USER spark
