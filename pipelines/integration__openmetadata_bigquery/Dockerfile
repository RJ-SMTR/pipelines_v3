FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/integration__openmetadata_bigquery ./pipelines/integration__openmetadata_bigquery/

COPY ./pipelines/common/ ./pipelines/common/

RUN sh ./pipelines/common/scripts/install-openmetadata.sh bigquery

RUN uv sync --all-packages
