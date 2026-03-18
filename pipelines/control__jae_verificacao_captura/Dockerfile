FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/control__jae_verificacao_captura ./pipelines/control__jae_verificacao_captura/

COPY ./pipelines/capture__jae_auxiliar ./pipelines/capture__jae_auxiliar/

COPY ./pipelines/capture__jae_gps_validador ./pipelines/capture__jae_gps_validador/

COPY ./pipelines/capture__jae_lancamento ./pipelines/capture__jae_lancamento/

COPY ./pipelines/capture__jae_transacao ./pipelines/capture__jae_transacao/

COPY ./pipelines/capture__jae_transacao_riocard ./pipelines/capture__jae_transacao_riocard/

COPY ./pipelines/common/ ./pipelines/common/

RUN uv sync --all-packages
