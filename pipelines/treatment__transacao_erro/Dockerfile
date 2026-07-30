FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__transacao_erro ./pipelines/treatment__transacao_erro/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./pipelines/treatment__operadora_cnpj/ ./pipelines/treatment__operadora_cnpj/

COPY ./pipelines/treatment__cadastro ./pipelines/treatment__cadastro/

COPY ./pipelines/capture__jae_auxiliar ./pipelines/capture__jae_auxiliar/

COPY ./pipelines/capture__jae_transacao_erro ./pipelines/capture__jae_transacao_erro/


RUN uv sync --all-packages
