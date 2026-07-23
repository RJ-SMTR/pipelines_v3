FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__riorotativo ./pipelines/treatment__riorotativo/

COPY ./pipelines/capture__riorotativo_credenciados ./pipelines/capture__riorotativo_credenciados/

COPY ./pipelines/capture__riorotativo_vagas ./pipelines/capture__riorotativo_vagas/

COPY ./pipelines/common/ ./pipelines/common/

RUN UV_CACHE_DIR=/tmp/openmetadata-uv-cache \
    UV_PYTHON_INSTALL_DIR=/opt/openmetadata-python uv python install 3.11 \
    && UV_CACHE_DIR=/tmp/openmetadata-uv-cache \
    UV_PYTHON_INSTALL_DIR=/opt/openmetadata-python uv venv \
    --python 3.11 /opt/openmetadata \
    && UV_CACHE_DIR=/tmp/openmetadata-uv-cache uv --no-config pip install --no-cache \
    --python /opt/openmetadata/bin/python \
    "openmetadata-ingestion[dbt]==1.13.1.0" \
    && /opt/openmetadata/bin/metadata --version \
    && rm -rf /tmp/openmetadata-uv-cache

RUN uv sync --all-packages
