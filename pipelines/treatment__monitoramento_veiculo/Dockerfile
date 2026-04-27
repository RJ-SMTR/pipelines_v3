FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__monitoramento_veiculo ./pipelines/treatment__monitoramento_veiculo/

COPY ./pipelines/capture__veiculo_fiscalizacao ./pipelines/capture__veiculo_fiscalizacao/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./queries ./queries/

RUN uv sync --all-packages
