FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__subsidio_sppo_apuracao ./pipelines/treatment__subsidio_sppo_apuracao/

COPY ./pipelines/control__jae_verificacao_captura ./pipelines/control__jae_verificacao_captura/

COPY ./pipelines/capture__jae_auxiliar ./pipelines/capture__jae_auxiliar/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./queries ./queries

RUN uv sync --all-packages
