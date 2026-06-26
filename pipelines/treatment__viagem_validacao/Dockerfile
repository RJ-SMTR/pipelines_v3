FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/treatment__viagem_validacao ./pipelines/treatment__viagem_validacao/

COPY ./pipelines/treatment__viagem_informada ./pipelines/treatment__viagem_informada/

COPY ./pipelines/treatment__planejamento_diario ./pipelines/treatment__planejamento_diario/

COPY ./pipelines/treatment__gps_conecta ./pipelines/treatment__gps_conecta/

COPY ./pipelines/treatment__gps_cittati ./pipelines/treatment__gps_cittati/

COPY ./pipelines/treatment__gps_zirix ./pipelines/treatment__gps_zirix/

COPY ./pipelines/capture__rioonibus_viagem_informada ./pipelines/capture__rioonibus_viagem_informada/

COPY ./pipelines/capture__conecta_registros ./pipelines/capture__conecta_registros/

COPY ./pipelines/capture__conecta_realocacao ./pipelines/capture__conecta_realocacao/

COPY ./pipelines/capture__cittati_registros ./pipelines/capture__cittati_registros/

COPY ./pipelines/capture__cittati_realocacao ./pipelines/capture__cittati_realocacao/

COPY ./pipelines/capture__zirix_registros ./pipelines/capture__zirix_registros/

COPY ./pipelines/capture__zirix_realocacao ./pipelines/capture__zirix_realocacao/

COPY ./pipelines/common/ ./pipelines/common/


RUN uv sync --all-packages && rm -f pyproject.toml uv.lock
