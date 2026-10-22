FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__subsidio_sppo_apuracao ./pipelines/treatment__subsidio_sppo_apuracao/

COPY ./pipelines/control__jae_verificacao_captura ./pipelines/control__jae_verificacao_captura/

COPY ./pipelines/capture__jae_auxiliar ./pipelines/capture__jae_auxiliar/

COPY ./pipelines/capture__jae_gps_validador ./pipelines/capture__jae_gps_validador/

COPY ./pipelines/capture__jae_lancamento ./pipelines/capture__jae_lancamento/

COPY ./pipelines/capture__jae_transacao ./pipelines/capture__jae_transacao/

COPY ./pipelines/capture__jae_transacao_erro ./pipelines/capture__jae_transacao_erro/

COPY ./pipelines/capture__jae_transacao_riocard ./pipelines/capture__jae_transacao_riocard/

COPY ./pipelines/common/ ./pipelines/common/


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
