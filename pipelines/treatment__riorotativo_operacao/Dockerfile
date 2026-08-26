FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/capture__jae_movimento_estacionamento_veiculo ./pipelines/capture__jae_movimento_estacionamento_veiculo/

COPY ./pipelines/capture__jae_estacionamento_veiculo ./pipelines/capture__jae_estacionamento_veiculo/

COPY ./pipelines/capture__jae_fiscalizacao_veiculo ./pipelines/capture__jae_fiscalizacao_veiculo/

COPY ./pipelines/treatment__riorotativo_auxiliar ./pipelines/treatment__riorotativo_auxiliar/

COPY ./pipelines/capture__riorotativo_credenciados ./pipelines/capture__riorotativo_credenciados/

COPY ./pipelines/capture__riorotativo_vagas ./pipelines/capture__riorotativo_vagas/

COPY ./pipelines/capture__jae_riorotativo_auxiliar ./pipelines/capture__jae_riorotativo_auxiliar/

COPY ./pipelines/treatment__riorotativo_operacao ./pipelines/treatment__riorotativo_operacao/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./queries ./queries

RUN uv sync --all-packages
