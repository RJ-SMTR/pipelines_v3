# Imagem minima para modelos dbt Python com OpenFisca no Dataproc Serverless.
#
# O Python precisa casar com o runtime do batch: runtime 1.1 = Spark 3.3.4 +
# Python 3.10. O cloudpickle do Spark 3.3 nao suporta Python >= 3.11
# (PicklingError: tuple index out of range); 3.13 removeu typing.io; 3.14
# segfaulta o PySpark 3.5. O runtime 1.1 e o default do dbt-bigquery e o unico
# validado neste ambiente — nao usar version "2.2"+ (quebra a escrita no BQ).
#
# Imagem propositalmente sem pandas/pyarrow: o codigo de materializacao do dbt
# importa pyspark.pandas dentro de try/except ImportError, entao sem pandas o
# wrapper segue o caminho de Spark DataFrame puro, sem risco de conflito com o
# Spark 3.3 (de 2022) montado pelo runtime.

# Debian 12 (bookworm), recomendado pelo guia de container customizado do Google
FROM python:3.10-slim-bookworm

# Utilitarios exigidos pelos scripts do Spark.
RUN apt-get update \
    && apt-get install -y --no-install-recommends procps tini \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN python -m pip install --no-cache-dir -r requirements.txt

ENV PYSPARK_PYTHON=/usr/local/bin/python

# O Dataproc Serverless executa o container como usuario spark (UID/GID 1099).
RUN groupadd -g 1099 spark && useradd -u 1099 -g 1099 -d /home/spark -m spark
USER spark
