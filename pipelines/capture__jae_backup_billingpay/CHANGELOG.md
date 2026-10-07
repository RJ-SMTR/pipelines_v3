# Changelog capture__jae_backup_billingpay

## [1.2.0] - 2026-10-06

### Adicionado

- Adiciona `ORDER BY` em todas as queries de captura (https://github.com/RJ-SMTR/pipelines_v3/pull/737)

### Corrigido

- Ajusta coluna de filtro da tabela `estacionamento_db.tipo_periodo_tarifa` (https://github.com/RJ-SMTR/pipelines_v3/pull/737)

## [1.1.5] - 2026-10-05

### Adicionado

- Adiciona backup do banco `estacionamento_db` (https://github.com/RJ-SMTR/pipelines_v3/pull/729)

## [1.1.4] - 2026-08-19

### Adicionado

- Adiciona tabelas no exclude e no filter (https://github.com/RJ-SMTR/pipelines_v3/pull/389)

## [1.1.3] - 2026-07-16

### Adicionado

- Adiciona tabelas no exclude e no filter (https://github.com/RJ-SMTR/pipelines_v3/pull/389)

## [1.1.2] - 2026-05-21

### Alterado

- Altera task `get_jae_db_config` para usar a nova função `get_jae_database_settings` (https://github.com/RJ-SMTR/pipelines_v3/pull/201)

## [1.1.1] - 2026-03-17

### Adicionado

- Altera lógica da timestamp do arquivo quando o parâmetro `end_datetime` é utilizado

## [1.1.0] - 2026-03-13

### Adicionado

- Cria possibilidade de adicionar as datas de inicio e fim na query customizada (https://github.com/RJ-SMTR/pipelines_v3/pull/81)

## [1.0.0] - 2026-03-12

### Adicionado

- Cria flow `capture__jae_backup_billingpay` (https://github.com/RJ-SMTR/pipelines_v3/pull/78)
