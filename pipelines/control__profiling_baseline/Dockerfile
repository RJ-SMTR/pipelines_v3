FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

# Sem COPY ./queries: este flow é apenas baseline e não usa dbt.

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/control__profiling_baseline ./pipelines/control__profiling_baseline/

COPY ./pipelines/common/ ./pipelines/common/

RUN uv sync --all-packages
