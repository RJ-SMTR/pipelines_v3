FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/capture__jae_movimento_estacionamento_veiculo ./pipelines/capture__jae_movimento_estacionamento_veiculo/

COPY ./pipelines/capture__jae_estacionamento_veiculo ./pipelines/capture__jae_estacionamento_veiculo/

COPY ./pipelines/capture__jae_fiscalizacao_veiculo ./pipelines/capture__jae_fiscalizacao_veiculo/

COPY ./pipelines/treatment__riorotativo_auxiliar ./pipelines/treatment__riorotativo_auxiliar/

COPY ./pipelines/capture__riorotativo_credenciados ./pipelines/capture__riorotativo_credenciados/

COPY ./pipelines/capture__riorotativo_vagas ./pipelines/capture__riorotativo_vagas/

COPY ./pipelines/capture__jae_riorotativo_auxiliar ./pipelines/capture__jae_riorotativo_auxiliar/

COPY ./pipelines/treatment__riorotativo_operacao ./pipelines/treatment__riorotativo_operacao/

COPY ./pipelines/treatment__riorotativo_ordem_pagamento ./pipelines/treatment__riorotativo_ordem_pagamento/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./queries ./queries

RUN UV_CACHE_DIR=/tmp/openmetadata-uv-cache \
    UV_PYTHON_INSTALL_DIR=/opt/openmetadata-python uv python install 3.11 \
    && UV_CACHE_DIR=/tmp/openmetadata-uv-cache \
    UV_PYTHON_INSTALL_DIR=/opt/openmetadata-python uv venv \
    --python 3.11 /opt/openmetadata \
    && UV_CACHE_DIR=/tmp/openmetadata-uv-cache uv --no-config pip install --no-cache \
    --python /opt/openmetadata/bin/python \
    "openmetadata-ingestion[dbt]==1.13.3.0" \
    && PYTHONHOME=/usr/local PYTHONPATH=/usr/local/lib/python3.13 \
    /opt/openmetadata/bin/python -I /opt/openmetadata/bin/metadata --version \
    && rm -rf /tmp/openmetadata-uv-cache

RUN uv sync --all-packages
