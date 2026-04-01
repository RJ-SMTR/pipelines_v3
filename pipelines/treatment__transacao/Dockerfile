FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__cadastro ./pipelines/treatment__cadastro/

COPY ./pipelines/capture__jae_auxiliar ./pipelines/capture__jae_auxiliar/

COPY ./pipelines/capture__jae_transacao ./pipelines/capture__jae_transacao/

COPY ./pipelines/capture__jae_transacao_riocard ./pipelines/capture__jae_transacao_riocard/

COPY ./pipelines/treatment__integracao ./pipelines/treatment__integracao/

COPY ./pipelines/treatment__transacao ./pipelines/treatment__transacao/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./queries ./queries

RUN uv sync --all-packages
