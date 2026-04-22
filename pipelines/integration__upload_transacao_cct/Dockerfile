FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/capture__cct_pagamento ./pipelines/capture__cct_pagamento/

COPY ./pipelines/integration__upload_transacao_cct ./pipelines/integration__upload_transacao_cct/


COPY ./pipelines/common/ ./pipelines/common/



RUN uv sync --all-packages
