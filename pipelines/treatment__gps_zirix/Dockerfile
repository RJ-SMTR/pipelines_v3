FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__gps_zirix ./pipelines/treatment__gps_zirix/

COPY ./pipelines/capture__zirix_registros ./pipelines/capture__zirix_registros/

COPY ./pipelines/capture__zirix_realocacao ./pipelines/capture__zirix_realocacao/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./queries ./queries

RUN uv sync --all-packages
