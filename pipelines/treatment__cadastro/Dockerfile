FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__cadastro ./pipelines/treatment__cadastro/

COPY ./pipelines/common/ ./pipelines/common/
COPY ./pipelines/capture__jae_auxiliar/ ./pipelines/capture__jae_auxiliar/

RUN uv sync --all-packages
