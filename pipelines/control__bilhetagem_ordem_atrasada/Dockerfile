FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/control__bilhetagem_ordem_atrasada ./pipelines/control__bilhetagem_ordem_atrasada/

COPY ./pipelines/integration__upload_transacao_cct ./pipelines/integration__upload_transacao_cct/

COPY ./pipelines/capture__jae_auxiliar ./pipelines/capture__jae_auxiliar/

COPY ./pipelines/treatment__cadastro ./pipelines/treatment__cadastro/

COPY ./pipelines/capture__jae_integracao ./pipelines/capture__jae_integracao/

COPY ./pipelines/capture__jae_transacao ./pipelines/capture__jae_transacao/

COPY ./pipelines/capture__jae_transacao_ordem ./pipelines/capture__jae_transacao_ordem/

COPY ./pipelines/capture__jae_ordem_pagamento ./pipelines/capture__jae_ordem_pagamento/

COPY ./pipelines/treatment__integracao ./pipelines/treatment__integracao/

COPY ./pipelines/treatment__transacao_ordem ./pipelines/treatment__transacao_ordem/

COPY ./pipelines/common/ ./pipelines/common/



RUN uv sync --all-packages && rm -f pyproject.toml uv.lock
