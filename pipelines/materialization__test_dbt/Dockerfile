FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/materialization__test_dbt ./pipelines/materialization__test_dbt/

COPY ./pipelines/common/ ./pipelines/common/

RUN uv sync --all-packages
