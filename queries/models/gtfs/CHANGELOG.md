# Changelog - gtfs

## [1.3.7] - 2026-01-08

### Adicionado

- Adiciona ao modelo `ordem_servico_trips_shapes_gtfs_v2` para considerar os serviços `SE393` e `SE550` no `Domingo` no feed do dia `2025-12-27` ` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1158)

### Corrigido

- Corrige o nome da coluna `extensao` para `distancia_planejada` do modelo  `ordem_servico_trajeto_alternativo_sentido_atualizado_aux_v3` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1158)

## [1.3.6] - 2026-01-07

### Corrigido

- Corrige o modelo `trips_filtrada_aux_gtfs` para buscar os eventos de `ordem_servico_trajeto_alternativo_sentido` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1156)

## [1.3.5] - 2026-01-06

### Alterado

- Altera os testes do gtfs dos modelos que não são mais utilizados (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1146)

- Altera o modelo `ordem_servico_trips_shapes_gtfs_v2.sql` para considerar os serviços de `Sabado` no `Domingo` no feed do dia `2025-12-27` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1146)


## [1.3.4] - 2025-12-26

### Adicionado

- Criado o modelo `ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs_v3.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1050)

### Alterado

- Alterado o modelo `ordem_servico_trips_shapes_gtfs.sql` para utilizar o `full outer union` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1050)

- Alterado o modelo `ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs.sql` para incluir o modelo `ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs_v3.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1050)

- Alterado o modelo `ordem_servico_trajeto_alternativo_gtfs.sql` para limitar a materialização dos dado pela variavel `queries/models/gtfs/DATA_GTFS_V5_INICIO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1050)

- Altera a lógica de associação para o tipo OS "Verão" no modelo `ordem_servico_trips_shapes_gtfs_v2` para considerar o servico `485` no sabado (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1141)

## [1.3.3] - 2025-12-23

### Alterado

- Altera a lógica de associação para o tipo OS "Verão" no modelo `ordem_servico_trips_shapes_gtfs_v2` para considerar serviços que geralmente não operam no dia. (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1133)

## [1.3.2] - 2025-12-21

### Adicionado

- Adiciona exceção para tipo OS `Verão` no modelo `ordem_servico_trips_shapes_gtfs_v2.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1132)

## [1.3.1] - 2025-12-22

### Corrigido

- Corrige a coluna `direction_id` no modelo `trips_gtfs.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1130)


## [1.3.0] - 2025-11-28

### Corrigido

- Corrige exceção para tipo OS `ENEM` e `V+ENEM` e refatora o modelo (DRY) `ordem_servico_trips_shapes_gtfs_v2.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1073)

## [1.2.9] - 2025-11-27

### Adicionado

- Adiciona exceção para tipo OS `ENEM` e `V+ENEM` no modelo `ordem_servico_trips_shapes_gtfs_v2.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1070)

### Corrigido

- Corrigida a alteração para tipo OS `ENEM` e `V+ENEM` no modelo `ordem_servico_trips_shapes_gtfs_v2.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1072)

## [1.2.8] - 2025-11-10

### Corrigido

- Corrigida a `distancia_planejada` dos trajetos alternativos do modelo `ordem_servico_trips_shapes_gtfs_v2.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1040)

### Alterado

- Alterada a condição de `data_versao_gtfs` para o modelo `ordem-servico_gtfs` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1042)

## [1.2.7] - 2025-10-30

### Adicionado

- Cria modelos `ordem_servico_diaria_gtfs.sql`, `ordem_servico_viagens_planejadas.sql` e `servicos_sentido_gtfs.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1016)

## [1.2.6] - 2025-08-08

### Alterado

- Adicionado o `indicador_duplo_sentido` no modelo `ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs_v2` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/776)

- Removida a condição para materialização incremental nos modelos efêmeros `ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs`, `ordem_servico_trips_shapes` e `trips_filtrada_aux_gtfs` e suas versões (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/776)

### Corrigido

- Corrigida a duplicação no modelo `ordem_servico_trips_shapes_v2` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/776)

## [1.2.5] - 2025-08-08

### Corrigido

- Corrigido o join com os trajetos alternativos no modelo `ordem_servico_trips_shapes_v2` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/773)

## [1.2.4] - 2025-08-07

### Corrigido

- Corrigida a verificação de shapes circulares no modelo `shapes_geom_gtfs` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/752)

### Alterado

- Alterados os modelos `ordem_servico_trips_shapes` e `ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs` para utilização do versionamento (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/752)

- Removido o union all com o modelo `ordem_servico_faixa_horaria_sentido` no modelo `ordem_servico_sentido_atualizado_aux_gtfs` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/752)

## [1.2.3] - 2025-05-29

### Alterado

- Inclui exceção de `shape_id` no modelo `shapes_geom_gtfs` em razão de trajetos alternativos de serviços circulares (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/598)

## [1.2.2] - 2025-04-30

### Alterado

- Desativa `ordem_servico_gtfs` em razão do novo modelo de OS (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/418)

- Refatora `ordem_servico_trips_shapes_gtfs` e `ordem_servico_sentido_atualizado_aux_gtfs` para remover dependência do `ordem_servico_gtfs` (desativado) (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/418)

## [1.2.1] - 2025-01-16

### Alterado

- Move modelos `ordem_servico_diaria.sql` e `servicos_sentido.sql` para o dataset `dashboard_operacao_onibus` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/389)

### Removido

- Modelo `ordem_servico_viagens_planejadas.sql` deletado

## [1.2.0] - 2024-12-04

### Alterado

- Inserido ajuste para o tipo_os `Enem` com feed_start_date `2024-09-29` e `2024-11-06` para considerar o planejamento do GTFS de sábado no domingo. Afetado o modelo `ordem_servico_trips_shapes_gtfs.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/354)

## [1.1.9] - 2024-09-10

### Alterado

- Altera modelo `ordem_servico_trips_shapes_gtfs` em razão da apuração por faixa horária (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)

### Corrigido
- Corrigido `schema.yml` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/202)

## [1.1.8] - 2024-08-02

### Alterado
- Adiciona tag `geolocalizacao` aos modelos `shapes_geom_gtfs.sql`, `shapes_gtfs.sql` e `stops_gtfs.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/127)

## [1.1.7] - 2024-07-23

### Adicionado

- Adiciona descrição da coluna `feed_update_datetime` em `feed_info` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/119)


## [1.1.6] - 2024-07-22

### Alterado

- Alterada a tabela `feed_info` de table para incremental e adicionada a coluna `feed_update_datetime`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/82)

- Alterada a tabela `fare_rules` para refletir a alteração nas primary keys (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/82)

## [1.1.5] - 2024-07-15

### Adicionado

- Adicionadas descrições das tabelas `servicos_sentido`, `ordem_servico_viagens_planejadas`, `ordem_servico_diaria` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/102)

## [1.1.4] - 2024-05-22

### Alterado

- Alterado tratamento das colunas `inicio_periodo` e `fim_periodo`, mantendo como STRING nos modelos `ordem_servico_sentido_atualizado_aux_gtfs2.sql` e `ordem_servico_trajeto_alternativo_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)
- Incluída coluna `tipo_os` no JOIN da CTE `ordem_servico_tratada` no modelo `ordem_servico_trips_shapes_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)
- Incluídos serviços com quantidade de partidas nulas no modelo `ordem_servico_sentido_atualizado_aux_gtfs.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)
- Alterado tratamento da coluna `evento` no modelo `ordem_servico_trajeto_alternativo_gtfs.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/305)

## [1.1.3] - 2024-05-21

### Corrigido
- Corrige atualização incremental da coluna `feed_end_date` nos modelos (https://github.com/prefeitura-rio/queries-rj-smtr/pull/321):
  - `agency_gtfs.sql`
  - `calendar_dates_gtfs.sql`
  - `fare_attributes_gtfs.sql`
  - `fare_rules_gtfs.sql`
  - `frequencies_gtfs.sql`
  - `routes_gtfs.sql`
  - `shapes_geom_gtfs.sql`
  - `shapes_gtfs.sql`
  - `stop_times_gtfs.sql`
  - `stops_gtfs.sql`
  - `trips_gtfs.sql`


## [1.1.2] - 2024-05-21

### Adicionado

- Adiciona coluna `tipo_os` no `schema.yml` em relação ao modelo `ordem_servico_trajeto_alternativo_gtfs.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/320)

### Corrigido

- Corrige tratamento da coluna `tipo_os` do modelo `ordem_servico_trajeto_alternativo_gtfs.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/320)

## [1.1.1] - 2024-05-21

### Adicionado

- Adiciona coluna `tipo_os` no modelo `ordem_servico_trajeto_alternativo_gtfs.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/318)

### Corrigido

- Corrige tratamento da coluna `tipo_os` do modelo `ordem_servico_gtfs.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/318)

## [1.1.0] - 2024-05-13

### Alterado

- Altera nomes das tabelas do dataset `gtfs` e suas referencias em outras tabelas removendo o 2 do final e o alias

## [1.0.2] - 2024-04-29

### Corrigido

- Corrige tratamento por partição do modelo `ordem_servico_trips_shapes_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/283)

## [1.0.1] - 2024-04-19

### Alterado

- Adiciona coluna `tipo_os` no modelo `ordem_servico_trips_shapes_gtfs2.sql` e atualiza descrição no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/278)

### Corrigido

- Corrige tratamento de colunas de tempo dos modelos `ordem_servico_sentido_atualizado_aux_gtfs2.sql` e `ordem_servico_trajeto_alternativo_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/278)

## [1.0.0] - 2024-04-18

### Adicionado

- Cria modelos:
  - `trips_filtrada_aux_gtfs2.sql` (`ephemeral`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
    - Neste modelo, é identificada uma trip de referência para cada serviço regular considerando a partição pela agregação das colunas `feed_version, trip_short_name, tipo_dia, direction_id`
    - Também são identificadas todas as trips de referência para os trajetos alternativos considerando a partição pela agregação das colunas `feed_version, trip_short_name, tipo_dia, direction_id, shape_id`
    - Em ambos os casos são ordenados por `feed_version, trip_short_name, tipo_dia, direction_id, shape_distance DESC`, privilegiando sempre a seleção dos trajetos mais longos
  - `ordem_servico_sentido_atualizado_aux_gtfs2.sql` (`ephemeral`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
  - `ordem_servico_trajeto_alternativo_sentido_atualizado_aux_gtfs2.sql` (`ephemeral`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
  - `ordem_servico_trajeto_alternativo_gtfs2.sql` (`incremental`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
  - `ordem_servico_trips_shapes_gtfs2.sql` (`incremental`) (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Adiciona descrição dos modelos `ordem_servico_trajeto_alternativo_gtfs2.sql` e `ordem_servico_trips_shapes_gtfs2.sql`, bem como informações sobre sua descontinuidade no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)

### Alterado

- Adiciona coluna `tipo_os` no modelo `ordem_servico_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
- Alterada descrição do modelo `feed_info_gtfs2.sql`, `shapes_geom_gtfs2.sql`, `ordem_servico_gtfs2.sql` no `schema.yml` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)

### Corrigido

- Refatora, otimiza e corrige quebra de shapes circulares no modelo `shapes_geom_gtfs2.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)
