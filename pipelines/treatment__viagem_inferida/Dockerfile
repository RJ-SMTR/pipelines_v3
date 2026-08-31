FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__viagem_inferida ./pipelines/treatment__viagem_inferida/

COPY ./pipelines/treatment__planejamento_diario ./pipelines/treatment__planejamento_diario/

COPY ./pipelines/treatment__gps_conecta ./pipelines/treatment__gps_conecta/

COPY ./pipelines/treatment__gps_maxtrack ./pipelines/treatment__gps_maxtrack/

COPY ./pipelines/treatment__gps_zirix ./pipelines/treatment__gps_zirix/

COPY ./pipelines/capture__conecta_registros ./pipelines/capture__conecta_registros/

COPY ./pipelines/capture__conecta_realocacao ./pipelines/capture__conecta_realocacao/

COPY ./pipelines/capture__maxtrack_registros ./pipelines/capture__maxtrack_registros/

COPY ./pipelines/capture__zirix_registros ./pipelines/capture__zirix_registros/

COPY ./pipelines/capture__zirix_realocacao ./pipelines/capture__zirix_realocacao/

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
