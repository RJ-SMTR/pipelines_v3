FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/control__openmetadata_dbt_artifacts ./pipelines/control__openmetadata_dbt_artifacts/

COPY ./pipelines/common/ ./pipelines/common/

RUN sh ./pipelines/common/scripts/install-openmetadata.sh dbt

RUN uv sync --all-packages
