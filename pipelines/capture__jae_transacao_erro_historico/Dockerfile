FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/capture__jae_transacao_erro_historico ./pipelines/capture__jae_transacao_erro_historico/

COPY ./pipelines/capture__jae_transacao_erro ./pipelines/capture__jae_transacao_erro/

COPY ./pipelines/common/ ./pipelines/common/

RUN uv sync --all-packages
