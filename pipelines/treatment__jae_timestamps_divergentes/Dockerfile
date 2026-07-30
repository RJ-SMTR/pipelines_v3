FROM ghcr.io/rj-smtr/pipelines_v3:latest

LABEL org.opencontainers.image.source=https://github.com/rj-smtr/pipelines_v3

WORKDIR /opt/prefect/pipelines_v3

COPY ./pyproject.toml ./uv.lock /opt/prefect/pipelines_v3/

COPY ./pipelines/capture__jae_auxiliar ./pipelines/capture__jae_auxiliar/

COPY ./pipelines/capture__jae_gps_validador ./pipelines/capture__jae_gps_validador/

COPY ./pipelines/capture__jae_lancamento ./pipelines/capture__jae_lancamento/

COPY ./pipelines/capture__jae_transacao ./pipelines/capture__jae_transacao/

COPY ./pipelines/capture__jae_transacao_erro ./pipelines/capture__jae_transacao_erro/

COPY ./pipelines/capture__jae_transacao_riocard ./pipelines/capture__jae_transacao_riocard/

COPY ./pipelines/treatment__operadora_cnpj/ ./pipelines/treatment__operadora_cnpj/

COPY ./pipelines/treatment__cadastro ./pipelines/treatment__cadastro/

COPY ./pipelines/treatment__extrato_cliente_cartao ./pipelines/treatment__extrato_cliente_cartao/

COPY ./pipelines/treatment__gps_validador ./pipelines/treatment__gps_validador/

COPY ./pipelines/treatment__integracao ./pipelines/treatment__integracao/

COPY ./pipelines/capture__jae_integracao/ ./pipelines/capture__jae_integracao/

COPY ./pipelines/treatment__financeiro_bilhetagem/ ./pipelines/treatment__financeiro_bilhetagem/

COPY ./pipelines/capture__jae_ordem_pagamento/ ./pipelines/capture__jae_ordem_pagamento/

COPY ./pipelines/treatment__transacao ./pipelines/treatment__transacao/

COPY ./pipelines/treatment__transacao_erro ./pipelines/treatment__transacao_erro/

COPY ./pipelines/treatment__passageiro_hora ./pipelines/treatment__passageiro_hora/

COPY ./pipelines/treatment__transacao_ordem ./pipelines/treatment__transacao_ordem/

COPY ./pipelines/capture__jae_transacao_ordem ./pipelines/capture__jae_transacao_ordem/

COPY ./pipelines/treatment__transacao_valor_ordem ./pipelines/treatment__transacao_valor_ordem/

COPY ./pipelines/control__jae_verificacao_captura ./pipelines/control__jae_verificacao_captura/

COPY ./pipelines/treatment__jae_timestamps_divergentes ./pipelines/treatment__jae_timestamps_divergentes/

COPY ./pipelines/common/ ./pipelines/common/

COPY ./queries ./queries

RUN uv sync --all-packages
