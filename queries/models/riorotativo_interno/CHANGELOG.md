# Changelog - riorotativo_interno

## [1.0.0] - 2026-07-16

### Adicionado

- Cria modelos:
  - `staging_movimento_estacionamento_veiculo_riorotativo.sql`
  - `staging_estacionamento_veiculo_riorotativo.sql`
  - `staging_fiscalizacao_veiculo_riorotativo.sql`
  - `staging_fiscalizacao_veiculo_imagem_riorotativo.sql`
  - `staging_veiculo_riorotativo.sql`
  - `staging_veiculo_cliente_riorotativo.sql`
  - `staging_notificacao_veiculo_riorotativo.sql`
  - `movimento_estacionamento_riorotativo.sql`
  - `verificacao_guardador_veiculo.sql`
  - `ordem_pagamento_riorotativo_guardador_veiculo_dia.sql`
  - `ordem_pagamento_riorotativo_entidade_dia.sql`

### Regras

- Divide períodos tarifados contínuos em subperíodos de duas horas e R$ 2,00.
- Classifica fiscalizações posteriores no mesmo subperíodo como ocorrência duplicada.
- Substitui `VAGA_INATIVA` por `VAGA_FORA_VIGENCIA` e `VAGA_FORA_FUNCIONAMENTO`.
- Usa a inclusão da imagem como proxy do momento da fotografia.
- Mantém um único motivo conforme a prioridade oficial definida.
