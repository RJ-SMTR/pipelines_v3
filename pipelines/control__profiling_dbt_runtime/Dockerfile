FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

# NOTE: a pasta ./queries NÃO é copiada de propósito.
# Este pipeline baixa queries do repositório em runtime (fetch_queries) e
# roda `dbt deps` no flow, validando a remoção da pasta queries da imagem.

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/control__profiling_dbt_runtime ./pipelines/control__profiling_dbt_runtime/

COPY ./pipelines/common/ ./pipelines/common/

RUN uv sync --all-packages
