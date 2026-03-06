FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__viagem_informada ./pipelines/treatment__viagem_informada/

COPY ./pipelines/treatment__planejamento_diario ./pipelines/treatment__planejamento_diario/

COPY ./pipelines/capture__rioonibus_viagem_informada ./pipelines/capture__rioonibus_viagem_informada/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./queries ./queries/

RUN uv sync --all-packages
