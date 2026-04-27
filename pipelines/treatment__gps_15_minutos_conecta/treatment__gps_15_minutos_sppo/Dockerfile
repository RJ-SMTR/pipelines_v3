FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__gps_15_minutos_sppo ./pipelines/treatment__gps_15_minutos_sppo/

COPY ./pipelines/capture__sppo_registros ./pipelines/capture__sppo_registros/

COPY ./pipelines/capture__sppo_realocacao ./pipelines/capture__sppo_realocacao/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./queries ./queries

RUN uv sync --all-packages
