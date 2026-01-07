# Changelog - bilhetagem

## [3.3.4] - 2025-12-15

### Corrigido

- Corrige adição de 4% em transações RioCard nos flows `aux_passageiro_hora.sql` e `passageiro_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1111)


## [3.3.3] - 2025-11-19

### Adicionado

- Adiciona coluna `id_laudo_pcd` no modelo `aux_laudo_pcd.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1066)

### Alterado

- Altera lógica do `id_unico` do modelo `aux_gratuidade_info.sql` para utilizar os ids unicos das outras tabelas auxiliares anteriores (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1066)
- Altera deduplicação e `id_unico` do modelo `aux_laudo_pcd.sql` trocando `datetime_inclusao` por `id_laudo_pcd` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1066)

### Corrigido

- Corrige range de inicio e fim de validade do modelo `aux_gratuidade_info.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1066)

## [3.3.2] - 2025-11-03

### Alterado

Altera filtro dos testes not null das colunas `id_validador` e `data_ordem` no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1022)

## [3.3.1] - 2025-09-29

### Alterado

- Altera janela de tempo para ler partições modificadas da transacao_ordem e transacao_retificada no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)

## [3.3.0] - 2025-09-16

### Adicionado

- Cria os modelos (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/875):
  - `staging_escola.sql`
  - `staging_estudante.sql`
  - `staging_laudo_pcd.sql`
  - `aux_escola_rede_ensino_atualizado.sql`
  - `aux_estudante.sql`
  - `aux_laudo_pcd.sql`
  - `aux_gratuidade_info.sql`

- Adiciona a coluna `id_cre_escola` no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/875)

- Adiciona transações de `TEC` no modelo `aux_passageiro_hora.sql`

### Alterado

- Altera fonte dos dados de gratuidade no modelo `transacao.sql` para `aux_gratuidade_info` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/875)

### Removido

- Remove colunas de estudante e laudo pcd dos modelos `staging_gratuidade.sql` e `aux_gratuidade.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/875)

## [3.2.12] - 2025-09-03

### Adicionado

- Adiciona teste `dbt_utils.expression_is_true__transacao_valor_ordem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/836)

## [3.2.11] - 2025-09-01

### Alterado

- Filtra transações do tipo `Botoeira` no modelo `transacao_valor_ordem.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/824)

### Corrigido

- Corrige tratamento das colunas `tipo_usuario` e `subtipo_usuario` no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/824)

## [3.2.10] - 2025-09-01

### Alterado

- Move modelos do dataset `br_rj_riodejaneiro_bilhetagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/822)

## [3.2.9] - 2025-08-27

### Alterado

- Define valor das transações do RioCard como `null` para datas anteriores a `2025-08-02` no modelo `aux_passageiro_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/817)

## [3.2.8] - 2025-08-27

### Alterado

- `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/812):
  - Altera a fonte dos dados de cliente para o modelo `cliente_jae.sql`
  - Altera classificação das colunas de subtipo do usuário para gratuidades de estudantes

### Corrigido

- Corrige gratuidades sendo com `tipo_usuario` pagante na `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/812)

## [3.2.7] - 2025-08-21

### Adicionado

- Adiciona coluna `subtipo_usuario_protegido` no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/804)

### Alterado

- Altera `tipo_usuario` PCD para Saúde no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/804)
- Remove subtipo quando a transação não for gratuidade no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/804)


## [3.2.6] - 2025-08-20

### Adicionado

- Adiciona testes not_null nos modelos de `bilhetagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/783):
  - `transacao.sql`
  - `transacao_riocard.sql`
  - `transacao_valor_ordem.sql`
  - `transacao_retificada.sql`
  - `integracao.sql`
  - `passageiro_hora.sql`
  - `passageiro_tile_hora.sql`

- Adiciona testes unique nos modelos de `bilhetagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/783):
  - `transacao.sql`
  - `gps_validador.sql`
  - `gps_validador_van.sql`

- Adiciona testes not_null nos modelos de `br_rj_riodejaneiro_bilhetagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/783):
  - `gps_validador.sql`
  - `gps_validador_van.sql`

### Alterado

- Ajusta a coluna `tipo_transacao`, `produto` e `tipo_usuario` no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/783)

## [3.2.5] - 2025-08-07

### Alterado

- Ajusta a coluna `tipo_transacao` no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/768)

## [3.2.4] - 2025-08-04

### Alterado

- Trata dados de id inteiro nos modelos de staging da Jaé (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/755)
- Altera tabela com informações de serviço da Jaé nos modelos `transacao.sql` e `transacao_riocard.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/755)

## [3.2.3] - 2025-08-04

### Adicionado

- Adiciona a coluna `valor_pagamento` no modelo `aux_passageiro_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/742)
- Adiciona a coluna `valor_total_transacao` no modelo `passageiro_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/742)

## [3.2.2] - 2025-08-03

### Alterado

- Inclui modo `Metrô` no modelo `aux_passageiro_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/751)

## [3.2.1] - 2025-07-30

### Removido

- Remove filtro `timestamp_captura` no modelo `aux_transacao_id_ordem_pagamento.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/740)

## [3.2.0] - 2025-07-03

### Adicionado

- Cria modelos (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/505):
   `aux_transacao_particao.sql`
   `dicionario_bilhetagem.sql`
   `integracao.sql`
   `passageiro_hora.sql`
   `passageiro_tile_hora.sql`
   `transacao_riocard.sql`
   `transacao.sql`

- Move modelos do dataset `br_rj_riodejaneiro_bilhetagem_staging` para `bilhetagem_staging` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/505):
  - `aux_gratuidade.sql`
  - `aux_passageiro_hora.sql`
  - `staging_gratuidade.sql`
  - `staging_integracao_transacao.sql`
  - `staging_produto.sql`
  - `staging_transacao_ordem.sql`
  - `staging_transacao_riocard.sql`
  - `staging_transacao.sql`

## [3.1.1] - 2025-07-03

### Adicionado

- Cria modelos `staging_transacao_retificada.sql` e `transacao_retificada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/658)

### Alterado

- Remove ordens incorretas do modelo `transacao_valor_ordem.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/658)

## [3.1.0] - 2025-02-04

### Adicionado
- Cria modelo `transacao_valor_ordem.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/411)

### Alterado
- Adiciona colunas com informação das ordens de pagamento no modelo `integracao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/411)
- Renomeia coluna `id` para `id_ordem_rateio` no modelo `staging_ordem_rateio.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/411)
- Remove coluna `id_ordem_ressarcimento` no modelo `ordem_pagamento_servico_operador_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/411)

## [3.0.0] - 2024-11-25

### Adicionado
- Cria os modelos `aux_transacao_id_ordem_pagamento.sql` e  `staging_transacao_ordem.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/333)

### Alterado
- Adiciona colunas de ordem de pagamento e `datetime_ultima_atualizacao` no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/333)
- Adiciona colunas de id único nos modelos `ordem_pagamento_consorcio_dia.sql` e `ordem_pagamento_servico_operador_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/333)

## [2.5.2] - 2024-11-13

### Alterado
- Cria coluna `datetime_ultima_atualizacao` no modelo `ordem_pagamento_consorcio_operador_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/321)
- Converte `timestamp_captura` para datetime no modelo `staging_ordem_pagamento_consorcio_operadora` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/321)

### Corrigido
- Ajusta lógica de preenchimento de valores pagos no modelo `ordem_pagamento_consorcio_operador_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/321)

## [2.5.1] - 2024-10-07

### Alterado
- Retira tratamento de valores nulos da coluna `tipo_gratuidade` no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/261)
- Adiciona tratamento de valores nulos na coluna `tipo_transacao_detalhe_smtr` no modelo `aux_passageiros_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/261)

## [2.5.0] - 2024-09-10

### Alterado
- Adiciona coluna `id_ordem_pagamento_consorcio_operador_dia` no modelo `ordem_pagamento_consorcio_operador_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/191)
- Cria tratamento da coluna `timestamp_captura` no modelo `staging_linha_consorcio_operadora_transporte.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/191)


## [2.4.0] - 2024-09-05

### Alterado
- Adiciona coluna `hash_cliente` no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/189)

## [2.3.0] - 2024-08-26

### Adicionado
- Cria modelos `staging_linha_consorcio_operadora_transporte.sql` e `staging_endereco.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/164)

### Alterado
- Utiliza data e hora no filtro incremental dos modelos (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/164):
  - `transacao.sql`
  - `transacao_riocard.sql`
  - `passageiros_hora.sql`
  - `passageiros_tile_hora.sql`
- Adiciona coluna geo_point_transacao nos modelos `transacao.sql` e `transacao_riocard.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/164)
- Altera materialized do modelo `aux_passageiros_hora.sql` de `table` para `ephemeral` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/164)
- Adiciona transações do RioCard no modelo `aux_passageiros_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/164)


## [2.2.1] - 2024-08-09

### Adicionado
- Adiciona descrição da coluna `id_validador` em `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/139)
- Adiciona descrição da coluna `quantidade_total_transacao` em `ordem_pagamento_consorcio_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/139)
- Adiciona descrição das colunas `quantidade_total_transacao` e `valor_pago` em `ordem_pagamento_consorcio_operador_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/139)
- Adiciona descrição da coluna `quantidade_total_transacao` em `ordem_pagamento_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/139)
- Adiciona descrição das colunas `tipo_gratuidade` e `tipo_pagamento` em `passageiros_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/139)
- Adiciona descrição das colunas `tipo_gratuidade` e `tipo_pagamento` em `passageiros_tile_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/139)

## [2.2.0] - 2024-08-05

### Adicionado
- Cria modelo `aux_h3_res9.sql` para tratar os dados geográficos de tile (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/130)
- Cria modelo `staging_linha_consorcio.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/130)

### Alterado
- `aux_passageiros_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/130)
  - Faz conversão dos dados geograficos no modelo
  - Altera materialized de `ephemeral` para `table`
  - Adiciona transações do tipo RioCard
- Muda tipo das colunas de valor do modelo `staging_transacao_riocard.sql` de `float` para `numeric` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/130)
- Altera lógica incremental do modelo `transacao_riocard.sql` para rodar de hora em hora (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/130)

## [2.1.4] - 2024-08-02

### Alterado
- Adiciona tag `geolocalizacao` aos modelos `gps_validador_van.sql` e `gps_validador.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/127)
- Adiciona tag `identificacao` ao modelo `staging_cliente.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/127)

## [2.1.3] - 2024-07-18

### Adicionado
- Adiciona transações de Van no modelo `passageiros_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/110)

### Alterado
- Define o tipo_gratuidade de transações do tipo `Gratuidade` que o cliente não foi encontrado na tabela `br_rj_riodejaneiro_bilhetagem_staging.aux_gratuidade` como `Não Identificado` no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/110)

## [2.1.2] - 2024-07-05

### Adicionado
- Adiciona coluna `versao_app` nos modelos `gps_validador_aux.sql`, `gps_validador.sql` e `gps_validador_van.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/96)

## [2.1.1] - 2024-06-19

### Corrigido
- Remove filtro de partições de gratuidade no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/69)

## [2.1.0] - 2024-06-13

### Alterado
- Adiciona colunas `data_pagamento` e `valor_pago` no modelo `ordem_pagamento_consorcio_operador_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/60)
- Subtrai valor pago a mais ao VLT (data_ordem = 2024-05-31) da ordem do dia 2024-06-07 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/60)
- Adiciona prioridade dos dados novos em relação aos antigos no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/60)

## [2.0.0] - 2024-06-11

### Adicionado
- Cria modelos `passageiros_hora_aux.sql` e `passageiros_tile_hora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/56)

### Alterado
- Adiciona colunas tipo_transacao_smtr e valor_pagamento no modelo `transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/56)
- Altera lógica da execução incremental no modelo `transacao.sql` para atualizar valores de rateio e o tipo de transação da primeira perna das integrações (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/56)

### Corrigido
- Corrige filtro no modelo `passageiros_hora_aux.sql` para adicionar transações de Ônibus (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/56)

## [1.2.1] - 2024-05-20

### Corrigido
- Altera alias da tabela `linha_sem_ressarcimento` no modelo `transacao.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/317)
- Corrige select servico no modelo `ordem_pagamento_servico_operador_dia.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/317)

## [1.2.0] - 2024-05-20

### Alterado
- Adiciona colunas `servico_jae` e `descricao_servico_jae` nos modelos (https://github.com/prefeitura-rio/queries-rj-smtr/pull/311):
  - `transacao.sql`
  - `integracao.sql`
  - `ordem_pagamento_servico_operador_dia.sql`
  - `passageiros_hora.sql`
  -  `gps_validador.sql`
  -  `gps_validador_van.sql`
  -  `staging/gps_validador_aux.sql`
- Adiciona coluna id_servico_jae nos modelos (https://github.com/prefeitura-rio/queries-rj-smtr/pull/311):
  -  `gps_validador.sql`
  -  `gps_validador_van.sql`
  -  `staging/gps_validador_aux.sql`

- Remove coluna `servico` no modelo de `staging/gps_validador_aux.sql` para pegar o dado da tabela de cadastro

## [1.1.0] - 2024-05-16

### Alterado
- Adiciona tratamento da coluna id_veiculo nos modelos ` transacao.sql` e `gps_validador.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/297)
- Adiciona coluna `quantidade_total_transacao` nos modelos `ordem_pagamento_consorcio_dia.sql`, `ordem_pagamento_consorcio_operador_dia.sql` e `ordem_pagamento_dia.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/297)
- Remove validação do modelo `ordem_pagamento_servico_operador_dia.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/297)

## [1.0.3] - 2024-05-03

### Corrigido
- Removido tratamento de arredondamento nos valores totais (https://github.com/prefeitura-rio/queries-rj-smtr/pull/294):
  - `bilhetagem.ordem_pagamento_dia`
  - `bilhetagem.ordem_pagamento_consorcio_operador_dia`
  - `bilhetagem.ordem_pagamento_consorcio_dia`

### Alterado
- Alterado cast de float para numeric (https://github.com/prefeitura-rio/queries-rj-smtr/pull/294):
  - `bilhetagem_staging.staging_ordem_pagamento`
  - `bilhetagem_staging.staging_ordem_pagamento_consorcio`
  - `bilhetagem_staging.staging_ordem_pagamento_consorcio_operadora`
  - `bilhetagem_staging.staging_ordem_rateio`
  - `bilhetagem_staging.staging_ordem_ressarcimento`

## [1.0.2] - 2024-04-18

### Alterado
- Filtra transações inválidas ou de teste no modelo `transacao.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/275)
  - Transações inválidas:
    - datas anteriores a 2023-07-17
  - Transações teste:
    - linhas sem ressarcimento
- Limita quantidade de ids listados no filtro da tabela de gratuidades no modelo `transacao.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/275)

## [1.0.1] - 2024-04-16

### Adicionado
- Adicionada descrições das colunas das tabelas: `ordem_pagamento_consorcio_dia`, `ordem_pagamento_consorcio_operador_dia`, `ordem_pagamento_dia`,`transacao_riocard`
- Adicionada a descrição da coluna intervalo_integracao na tabela `integracao`

### Corrigido
- deletada a tabela ordem_pagamento do schema

## [1.0.0] - 2024-04-05

### Adicionado
- Nova view para consultar os dados staging de transações do RioCard capturados pela Jaé: `br_rj_riodejaneiro_bilhetagem_staging/staging_transacao_riocard.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/254)
- Tabela tratada de transações do RioCard capturados pela Jaé: `transacao_riocard.sql` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/254)
