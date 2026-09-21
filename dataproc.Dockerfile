# Imagem mínima para modelos dbt Python com OpenFisca no Dataproc Serverless.

# Debian 12 (bookworm), recomendado pelo guia de container customizado do Google
FROM python:3.10-slim-bookworm

# Utilitários exigidos pelos scripts do Spark.
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps tini \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN python -m pip install --no-cache-dir -r requirements-dataproc.txt

ENV PYSPARK_PYTHON=/usr/local/bin/python

# O Dataproc Serverless executa o container como usuário spark (UID/GID 1099).
RUN groupadd -g 1099 spark && useradd -u 1099 -g 1099 -d /home/spark -m spark
USER spark