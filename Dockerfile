FROM alpine:3.22 AS get-instant-client-step

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

RUN wget -O /tmp/instantclient.zip "https://download.oracle.com/otn_software/linux/instantclient/2118000/instantclient-basic-linux.x64-21.18.0.0.0dbru.zip" \
    && unzip /tmp/instantclient.zip -d /tmp \
    && rm /tmp/instantclient.zip
FROM python:3.13-slim-bookworm

COPY --from=get-instant-client-step /tmp/instantclient_21_18 /opt/oracle/instantclient
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
COPY ./openssl.cnf /etc/ssl/openssl.cnf

ENV RUNNING_IN_DOCKER=1

RUN apt-get update \
    && apt-get install --no-install-recommends -y git curl gnupg2 libaio1 ca-certificates build-essential \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64,arm64,armhf signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install --no-install-recommends -y ffmpeg libsm6 libxext6 msodbcsql17 msodbcsql18 openssl unixodbc-dev \
    && sh -c "echo /opt/oracle/instantclient > /etc/ld.so.conf.d/oracle-instantclient.conf" \
    && ldconfig \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*
