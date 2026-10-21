# Changelog - treatment__gtfs

## [1.0.3] - 2026-10-21

### Alterado

- O título das notificações dos testes dbt no Google Chat e no Discord mostra o feed testado (`data_versao_gtfs`), em vez da janela de execução; no Google Chat, o nome do selector sai do título, como no Discord (https://github.com/RJ-SMTR/pipelines_v3/pull/786)

## [1.0.2] - 2026-10-19

### Adicionado

- Configura o webhook `google_chat_planejamento` para notificar no Google Chat as falhas dos testes dbt em produção, mantendo as notificações no Discord (https://github.com/RJ-SMTR/pipelines_v3/pull/773)

## [1.0.1] - 2026-10-14

### Removido

- Remove as descrições dos testes das constantes; as notificações usam as descrições definidas no dbt (https://github.com/RJ-SMTR/pipelines_v3/pull/763)

## [1.0.0] - 2026-10-08

### Adicionado

- Cria o flow de materialização do GTFS sobre o flow genérico de tratamento, com o selector `gtfs` (https://github.com/RJ-SMTR/pipelines_v3/pull/736)
