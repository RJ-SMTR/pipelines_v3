FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/teste__subflow ./pipelines/teste__subflow/

COPY ./pipelines/teste__flow_com_subflow ./pipelines/teste__flow_com_subflow/

COPY ./pipelines/common/ ./pipelines/common/



RUN uv sync --all-packages
