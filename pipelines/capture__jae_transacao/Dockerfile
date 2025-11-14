FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/capture__jae_transacao ./pipelines/capture__jae_transacao/

COPY ./pipelines/constants.py ./pipelines/constants.py

COPY ./pipelines/utils/ ./pipelines/utils/

COPY ./pipelines/default/capture/ ./pipelines/default/capture/

RUN uv sync --all-packages
