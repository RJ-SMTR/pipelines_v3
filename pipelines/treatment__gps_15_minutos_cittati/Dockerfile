FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__gps_15_minutos_cittati ./pipelines/treatment__gps_15_minutos_cittati/

COPY ./pipelines/capture__cittati_registros ./pipelines/capture__cittati_registros/

COPY ./pipelines/capture__cittati_realocacao ./pipelines/capture__cittati_realocacao/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./queries ./queries

RUN uv sync --all-packages
