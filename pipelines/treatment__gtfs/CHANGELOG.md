# Changelog - treatment__gtfs

## [1.0.3] - 2026-10-21

### Alterado

- O título das notificações dos testes dbt no Google Chat e no Discord mostra o feed testado (`data_versao_gtfs`), em vez da janela de execução; no Google Chat, o nome do selector sai do título, como no Discord (https://github.com/RJ-SMTR/pipelines_v3/pull/786)
- Os testes dbt notificados passam a incluir o `not_null` e o relationship de `shape_id` em `staging_trips`, que substituem o `test_shape_id_gtfs` de `trips_gtfs`, o teste de regex de `trip_headsign` e os relationships de `route_id` e `service_id` em `staging_trips`, os relationships de `stop_times` e `frequencies`, os relationships de `tipo_os` e `servico` entre os stagings de trajetos alternativos e de faixas horárias da OS e o teste de `extensao` positiva na faixa horária (https://github.com/RJ-SMTR/pipelines_v3/pull/781)

## [1.0.2] - 2026-10-19

### Adicionado

- Configura o webhook `google_chat_planejamento` para notificar no Google Chat as falhas dos testes dbt em produção, mantendo as notificações no Discord (https://github.com/RJ-SMTR/pipelines_v3/pull/773)

## [1.0.1] - 2026-10-14

### Removido

- Remove as descrições dos testes das constantes; as notificações usam as descrições definidas no dbt (https://github.com/RJ-SMTR/pipelines_v3/pull/763)

## [1.0.0] - 2026-10-08

### Adicionado

- Cria o flow de materialização do GTFS sobre o flow genérico de tratamento, com o selector `gtfs` (https://github.com/RJ-SMTR/pipelines_v3/pull/736)
