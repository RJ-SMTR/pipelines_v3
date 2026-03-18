FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__gps_conecta ./pipelines/treatment__gps_conecta/

COPY ./pipelines/capture__conecta_registros ./pipelines/capture__conecta_registros/

COPY ./pipelines/capture__conecta_realocacao ./pipelines/capture__conecta_realocacao/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./queries ./queries

RUN uv sync --all-packages
