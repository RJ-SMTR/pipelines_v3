# Plano de Migração: Prefect 1.4 → Prefect 3.0

> Ordenado por dependência: flows sem dependências primeiro, depois os que dependem deles.

## Grafo de Dependências

```
CAPTURA (sem dependências entre si)
├── cittati (registros + realocacao)
├── conecta (registros + realocacao)
├── zirix (registros + realocacao)
├── veiculo_fiscalizacao_lacre
├── rioonibus viagem_informada
├── sonda viagem_informada
├── stu tabelas
├── inmet temperatura
│
├── jae transacao               ← bilhetagem
├── jae transacao_riocard       ← bilhetagem
├── jae transacao_retificada    ← bilhetagem
├── jae integracao              ← bilhetagem
├── jae ordem_pagamento         ← bilhetagem
├── jae transacao_ordem         ← bilhetagem
├── cct pagamento               ← bilhetagem
│
├── brt_gps (captura, migration/)
├── sppo_gps (captura + realocacao, migration/)
├── gtfs (captura, migration/)
├── rdo (captura FTP, migration/)
├── recursos (captura Movidesk, migration/)
├── viagem_zirix (captura, migration/)
├── veiculo (licenciamento + infracao FTP, migration/)
└── controle_financeiro (CB + CETT + arquivo retorno, migration/)

TREATMENT - Nível 0 (dependem apenas de capturas)
├── gps_conecta           ← conecta (registros + realocacao)
├── gps_cittati           ← cittati (registros + realocacao)
├── gps_zirix             ← zirix (registros + realocacao)
├── gps_15min_conecta     ← conecta (registros + realocacao)
├── gps_15min_cittati     ← cittati (registros + realocacao)
├── gps_15min_zirix       ← zirix (registros + realocacao)
├── monitoramento_veiculo ← veiculo_fiscalizacao_lacre
├── planejamento_diario   ← (sem wait de captura)
├── infraestrutura        ← (sem wait)
├── datario               ← (sem wait)
├── matriz_integracao     ← (sem wait)
│
├── financeiro_bilhetagem ← cadastro + ordem_pagamento sources     ← bilhetagem
├── integracao            ← cadastro + integracao source            ← bilhetagem
├── pagamento_cct         ← cct pagamento sources                  ← bilhetagem
│
├── brt_gps (treatment)   ← brt_gps (captura)
├── sppo_gps (treatment)  ← sppo_gps (captura)
├── diretorios            ← (sem wait)
├── rdo (treatment)       ← rdo (captura)
├── recursos (treatment)  ← recursos (captura)
└── viagem_zirix (treat.) ← viagem_zirix (captura)

TREATMENT - Nível 1 (dependem de treatments nível 0)
├── viagem_informada      ← planejamento_diario + rioonibus captura
├── cadastro_veiculo      ← monitoramento_veiculo
├── monitoramento_temp.   ← monitoramento_veiculo
├── veiculo (sppo_veiculo_dia) ← veiculo capturas (via dbt)
├── viagens_sppo          ← sppo_gps treatment + gtfs (via dbt)
│
├── transacao             ← cadastro + transacao/riocard + integracao + auxiliares  ← bilhetagem
├── transacao_ordem       ← financeiro_bilhetagem + transacao_ordem source         ← bilhetagem
└── upload_transacao_cct  ← transacao_valor_ordem (via BQ)                         ← bilhetagem

TREATMENT - Nível 2 (dependem de treatments nível 1)
├── veiculo_dia           ← monitoramento_veiculo + cadastro_veiculo
├── viagem_validacao      ← viagem_informada + planejamento_diario + gps (conecta/cittati/zirix)
├── subsidio_sppo_apuracao ← viagens_sppo + veiculo (sppo_veiculo_dia)
│
├── passageiro_hora       ← transacao                                  ← bilhetagem
├── transacao_valor_ordem ← transacao_ordem + transacao + integracao   ← bilhetagem
└── validacao_dados_jae   ← transacao + integracao                     ← bilhetagem
```

---

## Captura

### Concluídos

- [x] `capture__serpro_autuacao` — Captura de autuações do SERPRO (PR #7)
- [x] `capture__jae_auxiliar` — Captura de tabelas auxiliares da Jaé (PR #16)
- [x] `capture__jae_gps_validador` — Captura de GPS do validador da Jaé (PR #41)
- [x] `capture__jae_lancamento` — Captura de lançamentos da Jaé (PR #52)
- [x] `capture__jae_transacao_erro` — Captura de transações com erro da Jaé (PR #39)
- [x] `capture__jae_transacao` — Captura de transações JAE (PR #60)
- [x] `capture__jae_transacao_riocard` — Captura de transações RioCard (PR #67)
- [x] `capture__jae_transacao_retificada` — Captura de transações retificadas (PR #71)
- [x] `capture__jae_integracao` — Captura de integração JAE (PR #58)
- [x] `capture__jae_ordem_pagamento` — Captura de ordem de pagamento JAE (PR #73)
- [x] `capture__rioonibus_viagem_informada` — Captura de viagem informada RioÔnibus (PR #61)
- [x] `capture__cittati_registros` — Captura de registros GPS ônibus (Cittati) (PR #83)
- [x] `capture__cittati_realocacao` — Captura de realocação GPS ônibus (Cittati) (PR #83)
- [x] `capture__conecta_registros` — Captura de registros GPS ônibus (Conecta) (PR #83)
- [x] `capture__conecta_realocacao` — Captura de realocação GPS ônibus (Conecta) (PR #83)
- [x] `capture__zirix_registros` — Captura de registros GPS ônibus (Zirix) (PR #83)
- [x] `capture__zirix_realocacao` — Captura de realocação GPS ônibus (Zirix) (PR #83)
- [x] `capture__jae_transacao_ordem` — Captura de transação/ordem JAE (PR #77)
- [x] `capture__jae_backup_billingpay` — Backup dados BillingPay (PR #78)
- [x] `capture__cct_pagamento` — Captura de pagamento CCT (5 tabelas)
- [x] `capture__stu_tabelas` — Captura de tabelas STU (21 tabelas)
- [x] `capture__jae_verificacao_ip` — Verificação de IP do banco JAE (Migrado como `control__jae_teste_ip`)
- [x] `capture__jae_verifica_captura` — Verificação de lacunas na captura JAE (Migrado como `control__jae_verificacao_captura`)
- [x] `capture__gtfs` — GTFS captura e tratamento
- [x] `capture__rioonibus_rdo_rho` — Captura de RDO/RHO RioÔnibus
- [x] `capture__sppo_registros` — Captura de registros GPS ônibus (SPPO)
- [x] `capture__sppo_realocacao` — Captura de realocação GPS ônibus (SPPO)
- [x] `capture__veiculo_infracao` — Captura de infrações de veículos
- [x] `capture__veiculo_licenciamento` — Captura de licenciamento de veículos
- [x] `capture__veiculo_sppo_registro_agente_verao` — Captura de registros agente verão

### Pendentes — Viagem / Monitoramento

- [ ] `capture__sonda_viagem_informada` — Captura de viagem informada BRT (Sonda)
  - Origem: `pipelines/capture/sonda/flows.py` → `CAPTURA_VIAGEM_INFORMADA_BRT`
  - Schedule: `10 7 * * *` (diário às 7:10)

- [ ] `capture__veiculo_fiscalizacao_lacre` — Captura de veículos/lacre (fiscalização)
  - Origem: `pipelines/capture/veiculo_fiscalizacao/flows.py` → `CAPTURA_VEICULO_LACRE`
  - Schedule: `0 5 * * *` (diário às 5h)
  - Downstream: `treatment__monitoramento_veiculo`
### Pendentes — Outros

- [ ] `capture__inmet_temperatura` — Captura de temperatura (INMET)
  - Origem: `pipelines/capture/inmet/flows.py` → `CAPTURA_TEMPERATURA_INMET`
  - Schedule: sem schedule (executado manualmente)

---

## Materialização / Treatment — Nível 0 (dependem apenas de capturas)

### Concluídos

- [x] `treatment__cadastro` — Materialização do selector `cadastro` (PR #17)
  - Waits: jae/linha, jae/cliente, etc. (capturas JAE já migradas)
- [x] `treatment__transito_autuacao` — Materialização do selector `transito_autuacao` + snapshot (PR #33)
- [x] `treatment__gps_validador` — Materialização do selector `gps_validador` (PR #42)
  - Waits: cadastro (treatment) + jae/gps_validador (captura já migrada)
- [x] `treatment__cliente_cpf` — Materialização do selector `cliente_cpf` (PR #28)
- [x] `treatment__extrato_cliente_cartao` — Materialização do selector `extrato_cliente_cartao` (PR #53)
- [x] `treatment__planejamento_diario` — Materialização de planejamento diário (PR #64)
- [x] `treatment__infraestrutura` — Materialização do selector `infraestrutura` (PR #72)
- [x] `treatment__passageiro_hora` — Materialização de passageiro/hora (PR #57)
- [x] `treatment__viagem_informada` — Materialização de viagem informada (PR #68)
- [x] `treatment__transacao_erro` — Materialização de transações com erro (PR #65)
- [x] `treatment__gps_conecta` — Materialização de GPS Conecta
- [x] `treatment__gps_cittati` — Materialização de GPS Cittati
- [x] `treatment__gps_zirix` — Materialização de GPS Zirix
- [x] `treatment__gps_15_minutos_conecta` — Materialização de GPS 15 minutos (Conecta)
- [x] `treatment__gps_15_minutos_cittati` — Materialização de GPS 15 minutos (Cittati)
- [x] `treatment__gps_15_minutos_zirix` — Materialização de GPS 15 minutos (Zirix)
- [x] `treatment__monitoramento_veiculo` — Materialização de monitoramento de veículos + snapshot
- [x] `treatment__financeiro_bilhetagem` — Materialização de financeiro bilhetagem
- [x] `treatment__integracao` — Materialização de integração bilhetagem
- [x] `treatment__pagamento_cct` — Materialização de pagamento CCT
- [x] `treatment__matriz_integracao_smtr` — Materialização de matriz de integração
- [x] `treatment__ordem_pagamento_quality_check` — Quality check bilhetagem (Migrado como `quality_check__ordem_pagamento`)
- [x] `treatment__gps_sppo` — Materialização de GPS SPPO
- [x] `treatment__gps_15_minutos_sppo` — Materialização de GPS 15 minutos (SPPO)

### Pendentes — Sem dependências de outros treatments

- [ ] `treatment__datario` — Materialização de dados do Datario
  - Origem: `pipelines/treatment/datario/flows.py` → `DATARIO_MATERIALIZACAO`
  - Schedule: sem schedule (executado manualmente)
  - Waits: nenhum

- [ ] `treatment__alerta_transacao` — Alerta de transação
  - Origem: `pipelines/treatment/validacao_dados_jae/flows.py` → `ALERTA_TRANSACAO_MATERIALIZACAO`
  - Schedule: sem schedule (executado manualmente)
  - Waits: nenhum

---

## Materialização / Treatment — Nível 1 (dependem de treatments nível 0)

### Concluídos

- [x] `treatment__monitoramento_temperatura` — Materialização de monitoramento de temperatura + snapshot
- [x] `treatment__transacao` — Materialização de transações bilhetagem
- [x] `treatment__transacao_ordem` — Materialização de transação/ordem bilhetagem
- [x] `upload_transacao_cct` — Upload de transação para PostgreSQL CCT (Migrado como `integration__upload_transacao_cct`)

### Pendentes

- [ ] `treatment__cadastro_veiculo` — Materialização do selector `cadastro_veiculo` + snapshot
  - Origem: `pipelines/treatment/cadastro/flows.py` → `CADASTRO_VEICULO_MATERIALIZACAO`
  - Schedule: `0 6 * * *` (diário às 6h)
  - Waits: `treatment__monitoramento_veiculo`
  - Post-tests: veiculo_licenciamento_dia

---

## Materialização / Treatment — Nível 2 (dependem de treatments nível 1)

### Concluídos

- [x] `treatment__transacao_valor_ordem` — Materialização de transação valor/ordem
- [x] `treatment__validacao_dados_jae` — Validação de dados JAE

### Pendentes

- [ ] `treatment__veiculo_dia` — Materialização de veículo/dia + snapshot
  - Origem: `pipelines/treatment/monitoramento/flows.py` → `VEICULO_DIA_MATERIALIZACAO`
  - Schedule: `15 6 * * *` (diário às 6:15)
  - Waits: `treatment__monitoramento_veiculo` + `treatment__cadastro_veiculo` (com delay de -168h)
  - Post-tests: veiculo_dia (not_null, unique, row_values, check_veiculo_lacre)

- [ ] `treatment__viagem_validacao` — Materialização de validação de viagem
  - Origem: `pipelines/treatment/monitoramento/flows.py` → `VIAGEM_VALIDACAO_MATERIALIZACAO`
  - Schedule: `0 8 * * *` (diário às 8h)
  - Waits: `treatment__viagem_informada` (delay -48h) + `treatment__planejamento_diario` + `treatment__gps_conecta` + `treatment__gps_cittati` + `treatment__gps_zirix`


---

## Migration (flows legados — captura + tratamento combinados)

> Estes flows estão em `pipelines/migration/` e têm arquitetura complexa com subflows.
> Ordenados por dependência: independentes primeiro, depois os que dependem de outros.

### Nível 0 — Independentes

- [ ] `capture__brt_gps` + `treatment__brt_gps` — GPS BRT (captura e materialização)
  - Origem: `pipelines/migration/br_rj_riodejaneiro_brt_gps/flows.py`
  - Flows: `captura_brt` (a cada minuto), `materialize_brt` (horário)
  - Dependências: nenhuma

- [x] `capture__sppo_gps` + `treatment__sppo_gps` — GPS SPPO ônibus (Migrado como `capture__sppo_registros`, `capture__sppo_realocacao` e `treatment__gps_sppo`)
- [ ] `treatment__diretorios` — Materialização de diretórios
  - Origem: `pipelines/migration/br_rj_riodejaneiro_diretorios/flows.py`
  - Flows: `diretorios_materializacao` (sem schedule, manual)
  - Dependências: nenhuma

- [x] `capture__rdo` + `treatment__rdo` — RDO captura e materialização (Migrado como `capture__rioonibus_rdo_rho`)

- [ ] `capture__recursos` + `treatment__recursos` — Subsídio recursos
  - Origem: `pipelines/migration/br_rj_riodejaneiro_recursos/flows.py`
  - Flows: `subsidio_sppo_recurso` (diário)
  - Dependências: nenhuma externa
  - Downstream: `treatment__subsidio_sppo_apuracao` (via dbt)

- [ ] `capture__viagem_zirix` + `treatment__viagem_zirix` — Viagens ônibus Zirix
  - Origem: `pipelines/migration/br_rj_riodejaneiro_viagem_zirix/flows.py`
  - Flows: `viagens_captura` (a cada 10 min), `viagens_recaptura` (horário), `viagem_zirix_materializacao` (horário :30)
  - Dependências: nenhuma

- [ ] `capture__controle_financeiro` — Controle financeiro CCT (bilhetagem)
  - Origem: `pipelines/migration/controle_financeiro/flows.py`
  - Flows: `controle_cct_cb_captura` (diário), `controle_cct_cett_captura` (diário), `arquivo_retorno_captura` (sexta-feira 16:30 BRT)
  - Dependências: nenhuma
  - Nota: captura de Google Sheets (CB, CETT) e API CCT (arquivo retorno)

### Nível 1 — Dependem de capturas/tratamentos nível 0

- [x] `capture__veiculo` — Veículo (licenciamento, infração, agente verão)
- [x] `treatment__viagens_sppo` — Viagens SPPO tratamento (Migrado como `treatment__sppo_viagens`)

### Nível 2 — Dependem de treatments nível 1

- [ ] `treatment__subsidio_sppo_apuracao` — Apuração de subsídio SPPO
  - Origem: `pipelines/migration/projeto_subsidio_sppo/flows.py` → `subsidio_sppo_apuracao`
  - Schedule: diário às 7:05
  - Dependências: `capture__veiculo` (subflow Prefect explícito: sppo_veiculo_dia), `treatment__viagens_sppo` (via dbt), GPS SPPO (via dbt pre-test), JAE capturas (transacao, transacao_riocard, gps_validador — via capture gap check)
  - Nota: flow mais complexo do repositório, com versionamento de apuração (V8→V9→V14)

---

## Bilhetagem — Processos Manuais / Orquestradores

> Flows executados manualmente ou sob demanda. Dependem de vários flows de bilhetagem.

- [ ] `ordem_atrasada` — Ordem de pagamento atrasada (captura + tratamento)
  - Origem: `pipelines/treatment/bilhetagem_processos_manuais/flows.py`
  - Schedule: sem schedule (manual)
  - Sequência: recaptura ordem_pagamento → captura → `treatment__financeiro_bilhetagem` → quality check → captura integração → `treatment__integracao` → captura transacao_ordem → `treatment__transacao_ordem`

- [ ] `timestamp_divergente_jae_recaptura` — Recaptura de timestamps divergentes
  - Origem: `pipelines/treatment/bilhetagem_processos_manuais/flows.py`
  - Schedule: sem schedule (manual)
  - Sequência: detecta lacunas → recaptura tabelas afetadas (transacao, transacao_riocard, gps_validador, lancamento, cliente) → rematerializa (cadastro, transacao, gps_validador, extrato_cliente_cartao) → verifica captura

---

## Migration — Bilhetagem (flows legados, schedules comentados)

> Todos os schedules estão comentados. São flows legados que foram substituídos pelos flows em `capture/jae/` e `treatment/bilhetagem/`.

- ~~`bilhetagem_transacao_captura`~~ — Substituído por `capture__jae_transacao`
- ~~`bilhetagem_transacao_riocard_captura`~~ — Substituído por `capture__jae_transacao_riocard`
- ~~`bilhetagem_fiscalizacao_captura`~~ — Legado, sem schedule
- ~~`bilhetagem_tracking_captura`~~ — Substituído por `capture__jae_gps_validador`
- ~~`bilhetagem_integracao_captura`~~ — Subflow legado
- ~~`bilhetagem_ressarcimento_captura`~~ — Subflow legado
- ~~`bilhetagem_auxiliar_captura`~~ — Substituído por `capture__jae_auxiliar`
- ~~`bilhetagem_materializacao_transacao`~~ — Substituído por `treatment__transacao`
- ~~`bilhetagem_materializacao_ordem_pagamento`~~ — Substituído por `treatment__financeiro_bilhetagem`
- ~~`bilhetagem_materializacao_integracao`~~ — Substituído por `treatment__integracao`
- ~~`bilhetagem_materializacao_gps_validador`~~ — Substituído por `treatment__gps_validador`
- ~~`bilhetagem_materializacao_dashboard_controle_vinculo`~~ — Legado, sem schedule
- ~~`bilhetagem_validacao_jae`~~ — Substituído por `treatment__validacao_dados_jae`
- ~~`bilhetagem_transacao_tratamento`~~ — Orquestrador legado, sem schedule
- ~~`bilhetagem_ordem_pagamento_captura_tratamento`~~ — Orquestrador legado, sem schedule
- ~~`bilhetagem_recaptura`~~ — Subflow legado

---

## Descontinuados / Sem necessidade de migração

- ~~`captura_stpl`~~ — GPS STPL descontinuado (sem dados), avaliar quando voltar
- ~~`stu_captura` (migration)~~ — Flow legado de STU, substituído por `capture__stu_tabelas`
- ~~`CADASTRO_MATERIALIZACAO`~~ — Sem schedule, substituído por `treatment__cadastro`
- ~~`TRANSITO_AUTUACAO_MATERIALIZACAO`~~ — Sem schedule, substituído por `treatment__transito_autuacao`
- ~~`backup_billingpay_historico`~~ — Sem schedule (comentado), backup histórico sob demanda

---

## Integração

### Concluídos

- [x] `integration__previnity_negativacao` — Integração com API Previnity (PR #8)

---

## Utilitários / Controle

- [x] `flow_set_key_redis` — Migrado como `control__set_redis_key`
- [x] `flow_source_freshness` — Migrado como `control__source_freshness`
