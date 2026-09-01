FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__remuneracao_openfisca ./pipelines/treatment__remuneracao_openfisca/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./queries ./queries

COPY ./openfisca_smtr ./openfisca_smtr/

RUN uv sync --all-packages \
    && uv pip install --python .venv/bin/python ./openfisca_smtr
