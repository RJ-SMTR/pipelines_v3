# Changelog - planejamento

## [1.6.9] - 2026-03-31

### Alterado

- Altera lógica das colunas `data_inicio_matriz` e `data_fim_matriz` no modelo `matriz_reparticao_tarifaria.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1356)

## [1.6.8] - 2026-03-23

### Alterado

- Alterado o `tipo_dia` no modelo `aux_calendario_manual.sql` de `2025-10-31` -> `Ponto Facultativo` conforme Recurso SMTR202511005101 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1337)


## [1.6.7] - 2026-03-19

### Alterado

- Alterados no modelo `aux_calendario_manual` o `tipo_os` de `2026-03-14` para `Term_Marg` conforme `Processo 000399.003413/2026-67` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1334)

## [1.6.6] - 2026-03-13

### Corrigido

- Corrigida a conversão do valor da coluna `partidas` para int64 no modelo `ordem_servico_faixa_horaria_sentido` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1324)

## [1.6.5] - 2026-03-09

### Alterado

- Alterados no modelo `aux_calendario_manual` o `tipo_os` de `2026-02-21`, `2026-02-22` para `Verão` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1304)

## [1.6.4] - 2026-02-25

### Alterado

- Altera lógica de interseção no modelo `segmento_shape` para impedir redução de buffer no primeiro e último segmento, recortando apenas segmentos intermediários (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1240)

## [1.6.3] - 2026-02-23

### Alterado

- Alterados no modelo `aux_calendario_manual` o `tipo_os` de `2026-02-07`, `2026-02-08`, `2026-02-14`, `2026-02-15`, `2026-02-16`, `2026-02-17` e `2026-02-18` para `Verão`, dia `2026-02-13` o `tipo_dia` para `Ponto Facultativo`, dias `2026-02-16` e `2026-02-17` o `tipo_dia` para `Domingo` e dia `2026-02-18` o `tipo_dia` para `Sabado`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1258)

## [1.6.2] - 2026-02-09

### Alterado

- Altera lógica incremental do modelo `viagem_planejada_planejamento` para `insert_overwrite` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1161)

## [1.6.1] - 2026-02-02

### Alterado

- Alterados no modelo `aux_calendario_manual` o `tipo_os` de `2026-01-17`, `2026-01-18`, `2026-01-31` e `2026-02-01` para `Verão` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1203)

## [1.6.0] - 2026-01-13

### Adicionado

- Cria modelos `tarifa_publica.sql` e `aux_matriz_transferencia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1162)

### Alterado

- Altera tarifa hardcoded no modelo `aux_matriz_integracao_modo.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1162)

## [1.5.9] - 2026-01-09

### Alterado

- Alterados no modelo `aux_calendario_manual` o `tipo_os` de `2025-12-20`, `2025-12-21`, `2025-12-27`, `2025-12-28`, `2026-01-03`, `2026-01-04`, `2026-01-10` e `2026-01-11` para `Verão` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1165)

## [1.5.8] - 2026-01-07

### Adicionado

- Corrige as colunas `sentido` e `evento` no modelo `ordem_servico_trajeto_alternativo_sentido` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1156)

## [1.5.7] - 2025-12-26

### Adicionado

- Adiciona as descrições do modelo `ordem_servico_faixa_horaria_sentido` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1119)
- Criado o modelo `ordem_servico_trajeto_alternativo_sentido`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1050)

## [1.5.6] - 2025-12-19

### Alterado

- Alterados no modelo `aux_calendario_manual` o `tipo_os` de `2025-12-06`, `2025-12-07`, `2025-12-13` e `2025-12-14` para `Verão` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1128)

## [1.5.5] - 2025-12-08

### Alterado

- Alterados no modelo `aux_calendario_manual` o tipo_dia de `2025-11-21` para `Ponto Facultativo`, o `tipo_os` de `2025-11-16` para `ENEM`, e o `tipo_os` dos dias `2025-11-22`, `2025-11-23`, `2025-11-29` e `2025-11-30` para `Verão` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1093)

## [1.5.4] - 2025-11-10

### Alterado

- Alterados o `tipo_dia` no modelo `aux_calendario_manual.sql` de `2025-10-20` -> `Ponto Facultativo` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1042)

## [1.5.3] - 2025-10-23

### Corrigido

- Corrigida a coluna `sentido` no modelo `ordem_servico_faixa_horaria_sentido` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/987)

## [1.5.2] - 2025-10-23

### Alterado

- Alterados o `tipo_dia` no modelo `aux_calendario_manual.sql` de `2025-10-04`, `2025-10-05` e `2025-10-12` -> `Atípico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/987)

## [1.5.1] - 2025-10-14

### Adicionado

- Adiciona alias no modelo `tuneis` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/950)
- Adiciona descrição das colunas para o modelo `tuneis` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/950)

## [1.5.0] - 2025-09-17

### Alterado

- Altera valor da integração de 0 para 4.7 no modelo `matriz_integracao.sql` e `aux_matriz_integracao_modo.sql`
- Renomeia coluna `valor_transacao` para `valor_integracao` no modelo `matriz_integracao.sql` e `aux_matriz_integracao_modo.sql`

## [1.4.10] - 2025-08-19

### Adicionado

- Adiciona filtro para dados do metrô no modelo `aux_matriz_integracao_modo.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/793)

## [1.4.9] - 2025-07-22

### Alterado

- Alterados o `tipo_dia` no modelo `aux_calendario_manual.sql` de `2025-07-04` e `2025-07-07` -> `Ponto facultativo` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/718)

## [1.4.8] - 2025-07-08

### Alterado

- Alterados o `tipo_dia` no modelo `aux_calendario_manual.sql` de `2025-06-19` -> `Domingo` e `2025-06-20` -> `Ponto facultativo` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/671)

## [1.4.7] - 2025-06-24

### Adicionado

- Cria modelo `tuneis` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/617)

### Alterado

- Altera referência da CTE `tunel` no modelo `segmento_shape` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/617)
- Altera lógica do modelo `segmento_shape` para considerar vigência da camada dos túneis (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/617)

## [1.4.6] - 2025-06-09

### Alterado

- Alterado o `subtipo_dia` 'Marcha para Jesus' para o dia `2025-05-24` no modelo `calendario` em razão da Operação Especial "Todo Mundo no Rio" - Lady Gaga [Processo.Rio MTR-PRO-2025/04520] (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/619)

## [1.4.5] - 2025-05-29

### Adicionado

- Adiciona o `subtipo_dia` 'Lady Gaga' para os dias `2025-05-03` e `2025-05-04` no modelo `calendario` em razão da Operação Especial "Todo Mundo no Rio" - Lady Gaga [Processo.Rio MTR-PRO-2025/04520] (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/598)

### Alterado

- Refatora referências do modelo `calendario` em variáveis (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/598)
- Altera tratamento da coluna servico no modelo `tecnologia_servico` para corrigir serviços com menos de 3 dígitos (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/600)

## [1.4.4] - 2025-05-28

### Adicionado

- Adiciona os dias `2025-05-02` como Ponto facultativo e `2025-05-03` e `2025-05-04` como Dia Atípico no modelo `aux_calendario_manual` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/597)

## [1.4.3] - 2025-05-12

### Adicionado

- Adiciona os dias `2025-04-18` como Feriado e `2025-04-22` como Ponto Facultativo no modelo `aux_calendario_manual` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/573)

## [1.4.2] - 2025-04-30

### Adicionado

- Cria modelo `aux_ordem_servico_diaria` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/418)

### Alterado

- Refatora `aux_ordem_servico_horario_tratado` para remover dependência do `ordem_servico_gtfs` (desativado) (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/418)

## [1.4.1] - 2025-04-10

### Corrigido

- Corrige query para filtrar feeds no modelo `calendario.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/536)
- Remove datas nulas no modelo `viagem_planejada_planejamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/536)

## [1.4.0] - 2025-04-08

### Adicionado

- Cria modelos `aux_matriz_integracao_modo.sql` e `matriz_reparticao_tarifaria.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/524)

### Alterado

- Altera modelo `matriz_integracao.sql` para a nova arquitetura da matriz (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/524)
- Altera modelos `staging_tecnologia_servico` e `tecnologia_prioridade` para padrão antigo da planilha de controle (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/530)

## [1.3.5] - 2025-04-01

### Adicionado

- Cria modelo `aux_stop_times_horario_tratado.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/481)
- Adiciona viagens da stop_times no modelo `viagem_planejada_planejamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/481)

### Alterado

- Simplifica tratamento dos horários nos modelos `aux_frequencies_horario_tratado.sql` e `aux_ordem_servico_horario_tratado.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/481)
- Adiciona tratamento de horários vazios no modelo `aux_ordem_servico_horario_tratado.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/481)
- Modifica filtro para não permitir feeds antigos em incrementais no modelo `viagem_planejada_planejamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/481)

## [1.3.4] - 2025-03-25

### Alterado

- Altera o tipo_os de `2025-03-08` e `2025-03-08` no modelo `aux_calendario_manual.sql` para `Extraordinária - Verão` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/502)

## [1.3.5] - 2025-03-27

### Corrigido

- Corrigido o tipo_dia de `2025-03-02` no modelo `aux_calendario_manual.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/501)

## [1.3.4] - 2025-03-25

### Alterado

- Altera o tipo_os e tipo_dia entre `2025-03-01` e `2025-03-05` no modelo `aux_calendario_manual.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/495)
- Altera modelo `tecnologia_servico` em razão da inclusão de vigência do tipo de tecnologia (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/493)

### Adicionado

- Cria modelos `staging_tecnologia_servico` e `tecnologia_prioridade` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/493)
- Cria modelos: `aux_os_sppo_faixa_horaria_sentido_dia`, `aux_ordem_servico_faixa_horaria` [Temporário] e `servico_planejado_faixa_horaria` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/458)

## [1.3.3] - 2025-03-14

### Adicionado

- Adiciona novas datas no modelo `aux_calendario_manual.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/480)

## [1.3.2] - 2025-03-10

### Alterado

- Ativa materialização do modelo `viagem_planejada_planejamento.sql` a partir de `2025-01-02` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/471)

## [1.3.1] - 2025-01-27

### Adicionado

- Cria modelo `tecnologia_servico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)

## [1.3.0] - 2024-12-30

### Adicionado

- Cria modelo `matriz_integracao.sql` com a matriz publicada pela SMTR (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/371)

## [1.2.0] - 2024-12-04

### Adicionado

- Cria modelos para tabela de viagens planejadas de acordo com o GTFS (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/352):
  - `aux_frequencies_horario_tratado.sql`
  - `aux_ordem_servico_horario_tratado.sql`
  - `aux_trips_dia.sql`
  - `viagem_planejada_planejamento.sql`

### Alterado

- Adiciona colunas start_pt e end_pt no modelo `shapes_geom_planejamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/352)

## [1.1.0] - 2024-11-08

### Adicionado

- Cria modelos para tabela de calendario: `aux_calendario_manual.sql` e `calendario.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/237)
- Cria modelos para divisão de shapes: `aux_segmento_shape.py`, `aux_shapes_geom_filtrada.sql`, `shapes_geom_planejamento.sql` e `segmento_shape.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/237)

## [1.0.2] - 2024-11-07

### Alterado

- Refatora modelo `ordem_servico_faixa_horaria` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/306)

## [1.0.1] - 2024-10-30

### Alterado

- Alterado modelo `ordem_servico_faixa_horaria` em razão das novas faixas horárias (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/295)

## [1.0.0] - 2024-08-29

### Adicionado

- Cria modelo `ordem_servico_faixa_horaria` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)
