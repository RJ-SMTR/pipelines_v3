FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__gps_validador ./pipelines/treatment__gps_validador/

COPY ./pipelines/treatment__cadastro ./pipelines/treatment__cadastro/

COPY ./pipelines/capture__jae_gps_validador ./pipelines/capture__jae_gps_validador/

COPY ./pipelines/common/ ./pipelines/common/

RUN uv sync --all-packages
