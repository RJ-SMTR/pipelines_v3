FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__integracao ./pipelines/treatment__integracao/

COPY ./pipelines/treatment__cadastro/ ./pipelines/treatment__cadastro/

COPY ./pipelines/treatment__financeiro_bilhetagem/ ./pipelines/treatment__financeiro_bilhetagem/

COPY ./pipelines/capture__jae_ordem_pagamento/ ./pipelines/capture__jae_ordem_pagamento/

COPY ./pipelines/capture__jae_integracao/ ./pipelines/capture__jae_integracao/

COPY ./pipelines/capture__jae_auxiliar/ ./pipelines/capture__jae_auxiliar/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./queries ./queries

RUN uv sync --all-packages
