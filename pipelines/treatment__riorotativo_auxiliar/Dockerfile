FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__riorotativo_auxiliar ./pipelines/treatment__riorotativo_auxiliar/

COPY ./pipelines/capture__riorotativo_credenciados ./pipelines/capture__riorotativo_credenciados/

COPY ./pipelines/capture__riorotativo_vagas ./pipelines/capture__riorotativo_vagas/

COPY ./pipelines/capture__jae_riorotativo_auxiliar ./pipelines/capture__jae_riorotativo_auxiliar/

COPY ./pipelines/common/ ./pipelines/common/

RUN uv sync --all-packages
