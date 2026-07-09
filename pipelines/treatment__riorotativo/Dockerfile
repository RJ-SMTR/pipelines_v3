FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__riorotativo ./pipelines/treatment__riorotativo/

COPY ./pipelines/common/ ./pipelines/common/


RUN uv sync --all-packages


#TODO: adicionar flow de captura dos dados do Rio Rotativo Digital