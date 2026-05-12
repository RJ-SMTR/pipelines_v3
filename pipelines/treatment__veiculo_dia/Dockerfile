FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__veiculo_dia ./pipelines/treatment__veiculo_dia/

COPY ./pipelines/capture__veiculo_sppo_registro_agente_verao/ ./pipelines/capture__veiculo_sppo_registro_agente_verao/

COPY ./pipelines/capture__veiculo_fiscalizacao_lacre ./pipelines/capture__veiculo_fiscalizacao_lacre/

COPY .pipelines/treatment__cadastro_veiculo ./pipelines/treatment__cadastro_veiculo/

COPY ./pipelines/treatment__monitoramento_veiculo ./pipelines/treatment__monitoramento_veiculo/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./queries ./queries

RUN uv sync --all-packages
