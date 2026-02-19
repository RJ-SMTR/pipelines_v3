# Changelog - monitoramento

## [1.9.2] - 2026-02-10

### Adicionado

- Migração dos modelos `gps_validador.sql`, `gps_validador_van.sql`, `gps_validador_aux.sql` e `staging_gps_validador.sql` para o repositório v3 (https://github.com/RJ-SMTR/pipelines_v3/pull/42)

## [1.9.1] - 2026-01-29

### Adicionado

- Adiciona `status` de Não licenciado para `tipo_veiculo` rodoviário no modelo `aux_veiculo_dia_consolidada`  [Processo nº 000300.003323/2026-18] (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1198)

## [1.9.0] - 2026-01-26

### Corrigido

- Corrige o teste `test_completude_temperatura`, ajustando a lógica para o intervalo de datas. (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1189)

### Adicionado

- Adiciona obrigatoriedade no filtro de partição nos modelos das tabelas `gps_validador.sql` e `gps_validador_van.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1186)

## [1.8.9] - 2026-01-22

### Adicionado

- Adiciona ao modelo `veiculo_dia` exceção ao prazo final de vistoria para o período entre `2026-01-01` e `2026-01-31` conforme RESOLUÇÃO SMTR Nº 3894 DE 29 DE DEZEMBRO DE 2025 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1183)

## [1.8.8] - 2026-01-19

### Corrigido

- Corrige o modelo `temperatura`, ajustando a lógica do join para utilizar dados do Alerta Rio quando não houver dados do INMET (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1176)

## [1.8.7] - 2026-01-13

### Alterado

- Altera data de processamento para a exceção de ajuste no modelo `veiculo_dia` para correção de tecnologia, conforme MTR-CAP-2025/59482 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1169)

## [1.8.6] - 2026-01-06

### Adicionado

- Adiciona ao modelo `autuacao_disciplinar_historico` a coluna `status` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1147)

### Alterado

- Altera o modelo `veiculo_dia` para exclusão de registros de autuações com `status` igual a "cancelado" (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1147)

## [1.8.5] - 2025-12-22

### Alterado

- Altera o modelo `veiculo_fiscalizacao_lacre` para correção duplicação de veículo (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1131)

## [1.8.4] - 2025-12-19

### Alterado

- Altera data de processamento para a exceção de ajuste no modelo `veiculo_dia` para correção de tecnologia, conforme MTR-CAP-2025/59482 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1129)

## [1.8.3] - 2025-12-15

### Alterado

- Altera data de processamento para a exceção de ajuste no modelo `veiculo_dia` para correção de tecnologia, conforme MTR-CAP-2025/59482 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1149)

## [1.8.2] - 2025-12-10

### Alterado

- Altera o filtro no modelo `veiculo_dia` para considerar `data` e `id_veiculo`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1100)
- Altera o teste `unique_combination_of_columns`para considerar `data`e `id_veiculo` do modelo `veiculo_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1100)

## [1.8.1] - 2025-12-04

### Alterado

- Altera exceção para tratamento da data_ultima_vistoria no modelo `veiculo_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1082)
- Altera a data de processamento no modelo `veiculo_dia` para corrigir a tecnologia apurada no período compreendido entre 2025-11-01 e 2025-11-15 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1083)

## [1.8.0] - 2025-12-03

### Alterado

- Altera data de processamento para a exceção de ajuste no modelo `veiculo_dia` para correção de tecnologia, conforme MTR-CAP-2025/59482 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1079)

### Corrigido

- Corrigido modelo `veiculo_fiscalizacao_lacre`ao adicionar o hífen ao regex e filtrando os veículos com placa corrigida. (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1081)

## [1.7.9] - 2025-12-02

### Alterado

- Acrescenta data de processamento para a exceção de ajuste no modelo `veiculo_dia` para correção de tecnologia, conforme MTR-CAP-2025/59482 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1078)

## [1.7.8] - 2025-11-11

### Adicionado

- Adicionada exceção para tratamento da data_ultima_vistoria no modelo `veiculo_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1048)

## [1.7.7] - 2025-11-04

### Alterado

- Remoção do filtro `indicador_ativa` da CTE `garagens` no modelo `aux_gps_parada` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1025)

## [1.7.6] - 2025-10-22

### Adicionado

- Cria modelos `temperatura_alertario` e `temperatura`, alteração conforme `MTR-MEM-2025/02796` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/977)

### Alterado

- Move modelo `temperatura_inmet` para staging (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/977)

## [1.7.5] - 2025-10-21

### Corrigido

- Corrigida a duplicidade de autuações de ar-condicionado(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/967)

## [1.7.4] - 2025-10-06

### Adicionado

- Adicionado o modelo de dicionário `dicionario_monitoramento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/926)

## [1.7.3] - 2025-10-01

### Adicionado

- Cria modelo `viagem_valida` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/910)

### Corrigido

- Corrige `indicador_campos_obrigatorios` no modelo `viagem_validacao` para invalidar viagens com `datetime_partida` maior que `datetime_chegada` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/910)
- Corrige divisão por 0 no cálculo da `velocidade_media` no modelo `viagem_validacao` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/910)

### Removido

- Ajusta o modelo efêmero `monitoramento_servico_dia` removendo a macro `is_incremental`.(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/907)

## [1.7.2] - 2025-09-25

### Corrigido

- Corrige coluna `timestamp_gps` para `datetime_gps` nos modelos `gps_viagem` e `gps_segmento_viagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/799)
- Corrige filtro da `partitions_query` no modelo `viagem_informada_monitoramento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/799)

### Adicionado

- Cria `indicador_campos_obrigatorios` no modelo `viagem_validacao` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/799)
- Adiciona coluna `datetime_captura_viagem` no modelo `gps_segmento_viagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/799)

### Alterado

- Altera lógica no modelo `viagem_informada_monitoramento` para particionar pela data do `datetime_partida` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/799)
- Altera `partition_filter` no modelo `aux_gps_realocacao` para teste diário do GPS (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/869)

## [1.7.1] - 2025-09-23

### Removido

- Remove teste `not_null` da coluna temperatura no modelo `temperatura_inmet` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/890)

## [1.7.0] - 2025-09-17

### Alterado

- Alterado o modelo `veiculo_fiscalizacao_lacre` para considerar atualizações de lacres anteriores ao início da tabela caso a coluna `datetime_ultima_atualizacao_fonte` seja superior a `2024-04-01` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/880)

### Adicionado

- Adicionada exceção para lacres adicionados após o prazo entre `2025-09-01` e `2025-09-18` no modelo `veiculo_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/880)

### Corrigido

- Corrigido os percentuais no `aux_veiculo_falha_ar_condicionado` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/874)

## [1.6.9] - 2025-09-15

### Corrigido

- Corrigido o modelo `monitoramento_servico_dia_v2` para a apuração por sentido (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/790)

## [1.6.8] - 2025-09-09

### Corrigido

- Corrige filtro no modelo `aux_gps_realocacao` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/857)

## [1.6.7] - 2025-09-01

### Alterado

- Move modelos do dataset `br_rj_riodejaneiro_bilhetagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/822)

## [1.6.6] - 2025-08-27

### Adicionado

- Cria o modelo `staging_temperatura_inmet` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/811)

### Alterado

- Altera o modelo `temperatura_inmet` para unir os dados `staging_temperatura_inmet` com `source("clima_estacao_meteorologica", "meteorologia_inmet")` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/811)
- Adicionada exceção para licenciamento entre `2025-08-01` e `2025-08-18` no modelo `veiculo_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/816)

## [1.6.5] - 2025-08-19

### Corrigido

- Corrige filtro no modelo `aux_veiculo_falha_ar_condicionado` para filtrar veículos que aplicam as regras de climatização (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/796)

### Alterado

- Altera lógica das colunas de controle no modelo `aux_veiculo_falha_ar_condicionado` para alterar a coluna `datetime_ultima_atualizacao` somente se a linha alterar (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/796)
- Refatora modelos `aux_veiculo_falha_ar_condicionado` e `veiculo_regularidade_temperatura_dia` para não materializar dados antes de `DATA_SUBSIDIO_V17_INICIO` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/796)

## [1.6.4] - 2025-08-15

### Alterado

- Refatora modelos `aux_veiculo_falha_ar_condicionado` e `veiculo_regularidade_temperatura_dia` para não materializar dados antes de `DATA_SUBSIDIO_V20_INICIO`, alteração referente ao Processo.rio `MTR-CAP-2025/25179` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/788)

## [1.6.3] - 2025-08-14

### Alterado

- Refatora modelos `aux_veiculo_falha_ar_condicionado` e `veiculo_regularidade_temperatura_dia`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/787)

## [1.6.2] - 2025-08-13

### Adicionado

- Adicionada exceção para lacres adicionados após o prazo entre `2025-07-16` e `2025-07-31` no modelo `veiculo_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/785)

### Corrigido

- Corrige teste `dbt_utils.relationships_where__id_auto_infracao__autuacao_disciplinar_historico` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/785)

## [1.6.1] - 2025-08-05

### Adicionado

- Adiciona teste `test_completude__temperatura_inmet` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/761)

## [1.6.0] - 2025-07-31

### Adicionado

- Cria modelos `aux_veiculo_falha_ar_condicionado`, `temperatura_inmet` e `veiculo_regularidade_temperatura_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/703)
- Adiciona coluna `ano_fabricacao` nos modelos `veiculo_dia` e `aux_veiculo_dia_consolidada` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/703)
- Adiciona teste dos modelos `aux_veiculo_falha_ar_condicionado`, `temperatura_inmet` e `veiculo_regularidade_temperatura_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/703)

### Alterado

- Refatora modelo `monitoramento_viagem_transacao` para utilizar os modelos `viagem_transacao_aux_v1` e `viagem_transacao_aux_v2` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/703)

## [1.5.9] - 2025-07-28

### Adicionado

- Adiciona testes de unicidade no modelo `veiculo_fiscalizacao_lacre.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/708)

## [1.5.8] - 2025-07-23

### Alterado

- Alterado o modelo `veiculo_fiscalizacao_lacre.sql` para lidar com correções dos dados capturados (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/719)

## [1.5.7] - 2025-07-10

### Alterado

- Alterada exceção para as datas de autuações disciplinares e dados de licenciamento de veículos no modelo `veiculo_dia.sql` para considerar tambem `data_inclusao_datalake` e `data_processamento` iguais a `2025-07-10`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/686)

## [1.5.6] - 2025-07-09

### Corrigido

- Corrigida a coluna `datetime_infracao` no modelo `staging_infracao` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/668)
- Corrigido os filtros dos testes do modelo `veiculo_fiscalizacao_lacre` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/681)
- Corrigido nome do teste do modelo `veiculo_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/681)

### Alterado

- Altera coluna `datetime_autuacao` no modelo `veiculo_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/682)

## [1.5.5] - 2025-07-08

### Alterado

- Altera filtro no modelo `veiculo_fiscalizacao_lacre` para não materializar dados com `id_auto_infracao` nulo (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/668)

## [1.5.4] - 2025-07-07

### Corrigido

- Corrigida a coluna `datetime_infracao` no modelo `staging_infracao.sql` para datas anteriores a `2025-06-25` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/666)

## [1.5.3] - 2025-06-30

### Corrigido

- Corrigida a verificação do status `Registrado com ar inoperante` no modelo `veiculo_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/652)

### Adicionado

- Adiciona `exclusion_condition` no teste `dbt_expectations.expect_row_values_to_have_data_for_every_n_datepart__veiculo_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/653)

## [1.5.2] - 2025-06-27

### Adicionado

- Adicionado limite de data para utilização dos dados do modelo `sppo_veiculo_dia.sql` no modelo`aux_veiculo_dia_consolidada.sql`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/648)
- Adicionada exceção para as datas de autuações disciplinares no modelo `veiculo_dia.sql` para considerar tambem autuações com `data_inclusao_datalake` = `2025-06-25`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/649)

## [1.5.1] - 2025-06-26

### Removido

- Remove a coluna `modo` do modelo `aux_veiculo_dia_consolidada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/648)

## [1.5.0] - 2025-06-25

### Adicionado

- Cria modelos `autuacao_disciplinar_historico.sql`, `aux_veiculo_dia_consolidada.sql`, `aux_veiculo_gps_dia.sql` e `veiculo_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)
- Adiciona coluna `id_execucao_dbt` no modelo `veiculo_fiscalizacao_lacre.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)

### Alterado

- Move modelo `infracao_staging.sql` do dataset `veiculo_staging` para `monitoramento_staging` e renomeia para `staging_infracao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)

## [1.4.2] - 2025-06-24

### Alterado

- Alterado o cálculo da `km_apurada` no modelo `monitoramento_servico_dia_v2.sql` para somar a quilometragem dos veículos não licenciados e não vistoriados (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/629)
- Altera lógica do modelo `gps_segmento_viagem` para considerar vigência da camada dos túneis (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/617)

## [1.4.1] - 2025-06-06

### Adicionado

- Adiciona testes nos modelos `staging_gps`, `staging_realocacao` e `gps` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/614)

## [1.4.0] - 2025-05-29

### Alterado

- Altera lógica de validação de viagens no modelo `viagem_validacao` [quantidade_segmentos_validos >= quantidade_segmentos_necessarios] (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/585)
- Altera modelo `gps_viagem` adicionando dados de GPS da API da Cittati (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/585)

## [1.3.9] - 2025-05-28

### Adicionado

- Cria modelos `staging_veiculo_fiscalizacao_lacre.sql` e `veiculo_fiscalizacao_lacre.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/594)

## [1.3.8] - 2025-05-28

### Adicionado

- Cria modelo `gps_15_minutos_union` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/586)

### Alterado

- Altera lógica do modelo `aux_gps_trajeto_correto` para considerar trajetos alternativos (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/595)

## [1.3.7] - 2025-05-20

### Adicionado

- Cria modelos `gps`, `gps_15_minutos`, `aux_gps`, `aux_gps_filtrada`, `aux_gps_parada`, `aux_gps_realocacao`, `aux_gps_trajeto_correto`, `aux_gps_velocidade` ,`aux_realocacao` e `staging_garagens`(https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/297)

## [1.3.6] - 2025-05-13

### Corrigido

- Corrige referências a modelos desabilitados nos modelos `monitoramento_servico_dia.sql`, `monitoramento_servico_dia_tipo_viagem.sql`, `monitoramento_servico_dia_historico.sql` e `monitoramento_servico_dia_tipo_viagem_historico.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/574)

## [1.3.5] - 2025-03-25

### Alterado

- Alterado a lógica dos modelos `gps_segmento_viagem` e `viagem_validacao` para o monitoramento de viagens (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/458)

## [1.3.4] - 2025-02-27

### Corrigido

- Corrige os valores as colunas `valor_subsidio_pago` e `valor_penalidade` antes da apuração por faixa no modelo `monitoramento_servico_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/465)

## [1.3.2] - 2025-02-21

### Alterado

- Torna filtro de partição obrigatório no modelo `gps_viagem.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/448)

## [1.3.1] - 2025-02-03

### Corrigido

- Transforma dados em branco em nulos no modelo `viagem_informada_monitoramento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/408)

## [1.3.0] - 2025-01-22

### Alterado

- Adiciona as colunas `indicador_servico_planejado_gtfs`, `indicador_servico_planejado_os`, `indicador_servico_divergente` e `indicador_shape_invalido` no modelo `viagem_validacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/392)

- Adiciona a coluna `indicador_servico_divergente` no modelo `gps_segmento_viagem.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/392)

- Remove tratamento de sentido no modelo `viagem_informada_monitoramento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/392)

### Corrigido

- Corrige trip_id inteiro nos modelos `staging_viagem_informada_rioonibus.sql` e `staging_viagem_informada_brt.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/392)

- Corrige filtro incremental no modelo `viagem_informada_monitoramento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/392)

## [1.2.2] - 2025-01-08

### Adicionado

- Cria modelos `sumario_servico_dia_pagamento_historico.sql` e `sumario_servico_dia_tipo_viagem_historico.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/258)

- Adicionado o label `dashboard` aos modelos `sumario_servico_dia_pagamento_historico.sql` e `sumario_servico_dia_tipo_viagem_historico.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/258)

## [1.2.1] - 2025-01-03

### Adicionado

- Cria modelo `monitoramento_viagem_transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/372)

## [1.2.0] - 2024-11-28

### Adicionado

- Cria modelo `staging_viagem_informada_brt.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/337)

### Alterado

- Adiciona viagens BRT no modelo: `gps_viagem.sql` e `viagem_informada_monitoramento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/337)
- Altera data hardcoded por variável no modelo `gps_viagem.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/337)
- Cria corte de viagens na execução full nos modelos `gps_viagem.sql` e `gps_segmento_viagem.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/337)

## [1.1.0] - 2024-11-08

### Adicionado

- Cria modelos de validação de viagens: `gps_viagem.sql`, `gps_segmento_viagem.sql` e `viagem_validacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/237)

### Alterado

- Adiciona coluna `modo` no modelo `viagem_informada_monitoramento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/237)

## [1.0.1] - 2024-10-23

### Corrigido

- Remove fuso horário na conversão para data do campo data_viagem no modelo `staging_viagem_informada_rioonibus.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/284)

## [1.0.0] - 2024-10-21

### Adicionado

- Cria modelos para tratamento de viagens informadas: `staging_viagem_informada_rioonibus.sql` e `viagem_informada_monitoramento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/276)
