# Changelog - dashboard_subsidio_sppo

## [8.1.9] - 2026-04-22

### Alterado

- Alterado o modelo `viagens_remuneradas_v2` para adicionar exceção no limite de viagens em serviços especificados no Processo SEI: 000399006676/2026-28 no período entre `2026-04-02T18:00:00` and `2026-04-02T23:59:59` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1405)

## [8.1.8] - 2026-04-08

### Alterado

- Alterado o modelo `viagens_remuneradas_v2` para adicionar exceção no limite de viagens em serviços especificados no Processo SEI: 000300.013241/2026-81 no período entre  `2026-03-16` e `2026-03-31` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1388)

## [8.1.7] - 2026-03-19

### Alterado

- Alterado o modelo `viagens_remuneradas_v2` para adicionar exceção no limite de viagens em serviços especificados no Processo SEI: 000300.009756/2026-87 no período entre  `2026-03-01` e `2026-03-15` e nos serviços especificados no Processo: 000399.002687/2026-39 na data de `2026-03-05` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1334)

## [8.1.6] - 2026-03-10

### Alterado

- Alterado o modelo `viagens_remuneradas_v2` para adicionar exceção no limite de viagens em serviços especificados no Processo n°000300.005989/2026-19 no período entre  `2026-02-16` e `2026-02-28` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1314)

## [8.1.5] - 2026-03-04

### Alterado

- Altera o modelo `viagens_remuneradas_v2` para adicionar os servicos `104`, `107`, `161`, `167`, `169`, `232`, `361`, `409`, `410`, `416`, `435`, `461`, `473`, `552`, `583`, `584`, `SP805`, `LECD127`, `LECD128`, `LECD129`, `LECD130`, `LECD131`, `LECD132`, `LECD133`, `LECD134` na exceção do limite de viagens em serviços especificados no período entre  `2026-01-16` e `2026-02-15` e os serviços `805`, `LECD136`, `LECD137` no período entre `2026-02-01` e `2026-02-15` conforme Processo n°000300.005989/2026-19  (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1292)

## [8.1.4] - 2026-02-27

### Alterado

- Altera a descrição da coluna `indicador_viagem_dentro_limite` adicionando limite de 110% (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1275)

## [8.1.3] - 2026-02-24

### Alterado

- Alterado o modelo `viagens_remuneradas_v2` para adicionar exceção no limite de viagens em serviços especificados no Processo n°000300.005989/2026-19 no período entre  `2026-02-01` e `2026-02-15` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1266)

## [8.1.2] - 2026-01-27

### Alterado

- Alterado o modelo `viagens_remuneradas_v2` para adicionar exceção no limite de viagens em serviços especificados no Processo n° 000300.001720/2026-55 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1195)

## [8.1.1] - 2026-01-15

### Alterado

- Move o modelo `valor_tipo_penalidade` para `subsidio` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1171)

## [8.1.0] - 2026-01-09

### Alterado

- Alterado o modelo `viagens_remuneradas_v2` para adicionar exceção no limite de viagens em serviços especificados no Processo nº 000300.000641/2026-27 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1165)

## [8.0.9] - 2026-01-08

### Alterado

- Alterado o modelo `viagens_remuneradas_v2` para adicionar uma exceção para o limite de viagens dos servicos `232`,`552`, `SP805`, `361`, `104`, `107`, `161`, `169`, `409`, `410`, `435`, `473`,   `583`,`584`, `109` de acordo com o Processo nº 000300.000641/2026-27 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1158)

## [8.0.8] - 2026-01-06

### Alterado

- Move a view `viagem_climatizacao.sql` para `dashboard_monitoramento_interno` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1121)

## [8.0.7] - 2025-11-25

### Adicionado

- Cria view `viagem_climatizacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1064)

## [8.0.6] - 2025-10-30

### Adicionado

- Cria modelo `sumario_servico_glosa_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1016)

## [8.0.5] - 2025-10-28

### Corrigido

- Corrigida a classificação de viagens acima do limite nos modelos `viagens_remuneradas_v1` e `viagens_remuneradas_v2` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1005)

## [8.0.4] - 2025-10-07

### Alterado

- Alterado o modelo `viagens_remuneradas_v2` para adicionar uma exceção para o limite de viagens dos servicos `161`, `LECD110`, `583`, `584` e `109` de acordo com o Processo.rio MTR-OFI-2025/06240 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/936)

## [8.0.3] - 2025-10-06

### Adicionado

- Adicionado o modelo de dicionário `dicionario_dashboard_subsidio_sppo` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/926)

## [8.0.2] - 2025-09-15

### Corrigido

- Corrigido modelo `sumario_servico_dia` para datas antes de `2023-09-16` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/790)
- Corrigida a utilização dos modelos `sumario_dia` e `sumario_servico_dia` no modelo `sumario_servico_dia_historico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/790)

## [8.0.1] - 2025-08-08

### Alterado

- Movidos os modelos `viagens_remuneradas_v1` e `viagens_remuneradas_v2` para a pasta staging (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/776)

## [8.0.0] - 2025-08-07

### Alterado

- Alterado o modelo `viagens_remuneradas` para apuração por sentido e para utilização do versionamento (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/752)

## [7.2.2] - 2025-07-31

### Alterado

- Alterado variável `DATA_SUBSIDIO_V15A_INICIO` por `DATA_SUBSIDIO_V17_INICIO` no modelo `viagens_remuneradas` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/703)

## [7.2.1] - 2025-07-08

### Adicionado

- Adiciona lógica condicional no modelo `viagens_remuneradas` para definir e utilizar a CTE `viagem_tecnologia` apenas quando `start_date` for anterior a `DATA_SUBSIDIO_V15_INICIO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/649)

## [7.2.0] - 2025-06-25

### Alterado

- Altera fonte dos dados de veículo para `aux_veiculo_dia_consolidada` no modelo `sumario_servico_tipo_viagem_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)

## [7.1.9] - 2025-06-24

### Alterado

- Altera referência da CTE `servico_faixa_km_apuracao` no modelo `viagens_remuneradas` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/624)
- Altera lógica do `subsidio_km` no modelo `viagens_remuneradas` conforme Art.3º da [RESOLUÇÃO SMTR Nº 3843/2025](https://doweb.rio.rj.gov.br/portal/visualizacoes/pdf/7371/#/p:14/e:7371) (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/624)

## [7.1.8] - 2025-06-12

### Corrigido

- Corrigidas as referências do modelo `sumario_servico_dia.sql`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/590)

### Adicionado

- Adicionada exceção para os serviços `583` e `584` no período de `2023-12-31` and `2024-01-01` no sentido Ida em razão do reprocessamento do TCM [MTR-CAP-2025/03003] (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/590)

## [7.1.7] - 2025-06-06

### Corrigido

- Corrigido o mínimo de viagens admitidas no teto de apuração em `viagens_remuneradas.sql` nos termos da [RESOLUÇÃO SMTR Nº 3843/2025](https://doweb.rio.rj.gov.br/portal/visualizacoes/pdf/7371/#/p:14/e:7371) (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/616)

## [7.1.6] - 2025-05-26

### Corrigido

- Corrigidos os períodos de aplicação das regras de apuração (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/593)

## [7.1.5] - 2025-05-09

### Adicionado

- Alterado o modelo `viagens_remuneradas.sql` para inclusão das novas regras de apuração  (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/572)

## [7.1.4] - 2025-04-30

### Alterado

- Refatora `viagens_remuneradas` para remover dependência do `ordem_servico_gtfs` (desativado) (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/418)

- Desativa `sumario_servico_dia_historico` para datas superiores ou iguais a `DATA_SUBSIDIO_V9_INICIO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/418)

## [7.1.3] - 2025-03-25

### Alterado

- Altera CTEs `tecnologias`, `prioridade_tecnologia` e `viagem_tecnologia` em razão da inclusão de vigência do tipo de tecnologia (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/493)

## [7.1.2] - 2025-03-10

### Adicionado

- Adiciona ordenação de viagens por datetime_partida no modelo `viagens_remuneradas.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/470)

## [7.1.1] - 2025-02-21

### Alterado

- Altera modelo `viagens_remuneradas` para não incrementar dados anteriores a `DATA_SUBSIDIO_V3A_INICIO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/447)

## [7.1.0] - 2025-02-17

### Corrigido

- Corrige teste de `distancia_planejada` maior que zero do modelo `viagens_remuneradas` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/433)

## [7.0.9] - 2025-01-28

### Corrigido

- Corrige case dentro da CTE subsidio_parametros no modelo `viagens_remuneradas` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/400)

### Alterado

- Altera lógica da coluna `valor_glosado_tecnologia` para converter o valor em NUMERIC no modelo `viagens_remuneradas` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/400)

## [7.0.8] - 2025-01-27

### Adicionado

- Altera regras do modelo `viagens_remuneradas` para apuração por tecnologia conforme DECRETO RIO N° 55631/2025 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)
- Adiciona valores atualizados das infrações no modelo `valor_tipo_penalidade` conforme MTR-MEM-2025/00005 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/390)

## [7.0.7] - 2024-10-30

### Adicionado

- Adiciona testes do subsidio para `sumario_servico_dia`, `sumario_servico_dia_historico`, `sumario_servico_dia_tipo`, `sumario_servico_dia_tipo_sem_glosa` e `viagens_remuneradas` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/256)

## [7.0.6] - 2024-11-05

### Alterado

- Altera regras do modelo `viagens_remuneradas` conforme Resolução SMTR N° 3777/2024 e MTR-MEM-2024/02465 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/300)

## [7.0.5] - 2024-10-16

### Corrigido

- Corrigido join em `sumario_servico_dia_historico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/271)

## [7.0.4] - 2024-09-27

### Corrigido

- Remove agrupamento de viagens planejadas em `sumario_servico_dia` e `sumario_servico_dia_tipo` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/250)

## [7.0.3] - 2024-09-20

### Alterado

- Remove kms de veículos "Não licenciado" e "Não vistoriado" do cálculo do pof na CTE `servico_faixa_km_apuracao` do modelo `viagens_remuneradas` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/239)

## [7.0.2] - 2024-08-29

### Alterado

- Alterado os modelos `sumario_dia`, `sumario_servico_dia` e `sumario_servico_dia_tipo` em razão de alterações no modelo `viagem_planejada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)

- Alterado modelo `viagens_remuneradas` em razão da apuração por faixa horária (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/114)

## [7.0.1] - 2024-08-19

## Adicionado

- Adicionada coluna `datetime_ultima_atualizacao` na tabela `sumario_servico_dia_historico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/147)

## [7.0.0] - 2024-07-31

### Alterado

- Refatora tabelas `viagens_remuneradas.sql` e
  `sumario_servico_dia_tipo.sql` para uso da nova tabela de
  referência (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/121)
- Adiciona novo tipo de viagem `"Sem transação"` nas tabelas
  `viagens_remuneradas.sql`,
  `sumario_servico_dia_tipo.sql`,
  `sumario_servico_dia_tipo_sem_glosa.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/121)

## [6.0.0] - 2024-04-18

### Alterado

- Adiciona novo filtro no modelo `sumario_servico_dia_historico.sql` em razão de alterações no modelo `viagem_planejada.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/261)

## [5.0.0] - 2024-03-30

### Adicionado

- Nova tabela de referência do valor do subsídio por tipo de viagem,
  inclusão do novo tipo `"Não vistoriado"`:
  `subsidio_valor_km_tipo_viagem.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)

### Alterado

- Refatora tabelas `viagens_remuneradas.sql` e
  `sumario_servico_dia_tipo_sem_glosa.sql` para uso da nova tabela de
  referência (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
- Adiciona novo tipo de viagem/status `"Não vistoriado"` nas tabelas
  `sumario_servico_dia_tipo.sql`,
  `sumario_servico_dia_tipo_sem_glosa.sql`,
  `sumario_servico_tipo_viagem_dia.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
- Simplifica padronização dos tipos de viagens nas tabelas
  `sumario_servico_dia_tipo.sql` e `sumario_servico_tipo_viagem_dia.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
- Atualiza documentação de tabelas e colunas (https://github.com/prefeitura-rio/queries-rj-smtr/pull/239)
- Alterações feitas em https://github.com/prefeitura-rio/queries-rj-smtr/pull/229 e https://github.com/prefeitura-rio/queries-rj-smtr/pull/236 corrigidas em https://github.com/prefeitura-rio/queries-rj-smtr/pull/239
