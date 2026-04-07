# Changelog - validacao_dados_jae

## [3.1.5] - 2026-03-31

### Adicionado

- Adiciona modos `Ônibus BUM` e `SPPO MNS` no modelo `aux_transacao_filtro_integracao_calculada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1356)

## [3.1.4] - 2026-01-28

### Alterado

- Adapta query do modelo `integracao_invalida.sql` para o novo formato da matriz de integração (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1182)

## [3.1.3] - 2026-01-13

### Alterado

- Altera tarifa hardcoded no modelo `aux_transacao_filtro_integracao_calculada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1162)
- Cria lógica para reconhecer a versão na matriz de integração com base na data da transação no modelo `aux_calculo_integracao.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1162)

## [3.1.2] - 2025-12-15

### Adicionado

- Adiciona coluna `classificacao_integracao_nao_realizada` no modelo `integracao_nao_realizada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1110)

### Removido

- Remove regra de serviço e sentido igual no modelo `aux_calculo_integracao.py` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1110)

### Corrigido

- Corrige incremental dos modelos `aux_integracao_calculada.sql` e `integracao_nao_realizada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1110)


## [3.1.1] - 2025-11-10

### Adicionado

- Adiciona colunas no modelo `transacao_invalida.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1038):
  - `indicador_processamento_anterior_transacao`
  - `indicador_integracao_fora_tabela`
  - `versao`
  - `datetime_ultima_atualizacao`
  - `id_execucao_dbt`

## [3.1.0] - 2025-10-13

### Adicionado

- Cria modelo `alerta_transacao.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/944)

## [3.0.2] - 2025-10-09

### Alterado

- Altera regra da coluna `modo` do modelo `aux_transacao_filtro_integracao_calculada` adicionando `BRT ESP` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/822)

## [3.0.1] - 2025-09-01

### Alterado

- Altera referências dos modelos de ordem de pagamento (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/822)

## [3.0.0] - 2025-08-19

### Adicionado

- Cria nova lógica para o cálculo de integrações não realizadas (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/793):
  - `aux_particao_calculo_integracao.sql`
  - `aux_transacao_filtro_integracao_calculada.sql`
  - `aux_calculo_integracao.py`
  - `aux_integracao_calculada.sql`
  - `integracao_nao_realizada.sql`

## [2.0.0] - 2024-12-30

### Alterado

- Altera modelos `integracao_nao_realizada.sql` e `integracao_invalida.sql` para considerar a matriz publicada pela SMTR (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/371)
- Altera variável de data dos modelos `integracao_nao_realizada.sql` e `integracao_invalida.sql` de `run_date` para `date_range_[start/end]` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/371)

## [1.1.3] - 2024-09-04

### Alterado
  - Modelo `integracao_nao_realizada.sql`:
    - Soma 1 na coluna `sequencia_integracao` para padronizar em relação a tabela `integracao` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/185)
    - Cria filtro para remover integrações com 2 ou mais transações do modo `BRT` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/185)

## [1.1.2] - 2024-08-27

### Corrigido
  - Remove transações do tipo gratuidade no modelo `integracao_nao_realizada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/169)

## [1.1.1] - 2024-08-21

### Corrigido
  - Remove pernas nulas no modelo `integracao_nao_realizada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/159)
  -
## [1.1.0] - 2024-08-21

### Adicionado
  - Cria modelo `integracao_nao_realizada.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/157)

## [1.0.0] - 2024-07-17

### Adicionado
  - Cria modelos para validação dos dados da Jaé (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/98):
    - `aux_transacao_ordem.sql`
    - `ordem_pagamento_servico_operador_dia_invalida.sql`
    - `ordem_pagamento_consorcio_operador_dia_invalida.sql`
    - `ordem_pagamento_consorcio_dia_invalida.sql`
    - `ordem_pagamento_dia_invalida.sql`
    - `integracao_invalida.sql`
    - `transacao_invalida.sql`
    - `ordem_pagamento_invalida.sql`
