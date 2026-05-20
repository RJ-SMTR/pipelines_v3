# Sincronização com upstream `rj-smtr/pipelines_v3`

> Gerado em 2026-05-20, antes do fast-forward de `master`.
> Base local: `52e915c` (PR #122 — captura veículo).
> Base upstream: `d8ea7ad` (PR #197 — "Registra flow").
> **222 commits atrás, 0 à frente → fast-forward limpo, sem conflitos.**
> 523 arquivos modificados, +27.908 / −1.136 linhas.

---

## 1. Como ficou a migração no upstream

O upstream avançou bastante: 22 novos pipelines, snapshots, modelos dbt de
subsídio importados, novos pipelines de manutenção e controle. O flow mais
crítico do fluxo de subsídio (`treatment__subsidio_sppo_apuracao`) **continua
pendente** no `MIGRATION_PLAN.md` upstream (linha 266), mas todas as suas
dependências já foram migradas.

### 1.1 Pipelines novos (22)

**Captura (7)**
- `capture__gtfs` — substitui `pipelines/migration/br_rj_riodejaneiro_gtfs/`
- `capture__inmet_temperatura`
- `capture__rioonibus_rdo_rho` — substitui `migration/br_rj_riodejaneiro_rdo/`
- `capture__sonda_registros`
- `capture__sonda_viagem_informada`
- `capture__sppo_realocacao`
- `capture__sppo_registros`
- `capture__veiculo_fiscalizacao_lacre`

**Treatment (11)**
- `treatment__cadastro_veiculo`
- `treatment__datario`
- `treatment__gps_15_minutos_sppo`
- `treatment__gps_sonda`
- `treatment__gps_sppo`
- `treatment__monitoramento_temperatura`
- `treatment__monitoramento_veiculo`
- `treatment__operadora_cnpj` — nova arquitetura de cadastro de operadores
- `treatment__sppo_viagens`
- `treatment__veiculo_dia`
- `treatment__viagem_validacao`

**Controle / Manutenção (3)**
- `control__model_freshness`
- `maintenance__db_retention`
- `maintenance__janitor`

Nenhum pipeline foi removido.

### 1.2 Modelos dbt importados (relevantes para subsídio)

Diretórios novos em `queries/models/`:

- `dashboard_subsidio_sppo/` (+ `staging/`, `deprecated/`)
- `dashboard_subsidio_sppo_staging/`
- `dashboard_subsidio_sppo_v2/` (+ `staging/`, `deprecated/`)
- `projeto_subsidio_sppo/` (+ `staging/`, `deprecated/`)
- `subsidio/` (+ `staging/`)
- `financeiro_interno/` (+ `staging/`) — contém:
  - `subsidio_faixa_servico_dia_tipo_viagem.sql` (+ `v1`, `v2`)
  - `subsidio_penalidade_servico_faixa.sql` (+ `v1`, `v2`)
  - `subsidio_sumario_servico_dia_pagamento.sql`
- `dashboard_monitoramento_interno/`
- `monitoramento_interno/` (+ `staging/`)
- `veiculo/` (+ `staging/`)
- `datario/`
- `gtfs/staging/`

Total: **70 arquivos relacionados a subsídio** entram no fork — toda a árvore
de apuração de subsídio (dbt) está agora presente.

### 1.3 Snapshots

Diretório `queries/snapshots/` recebe novos arquivos:
- `monitoramento/snapshot_temperatura*.sql`
- `monitoramento/snapshot_veiculo_fiscalizacao_lacre.sql`
- `monitoramento/snapshot_viagem_completa.sql`
- `monitoramento/snapshot_viagem_planejada.sql`
- `veiculo/snapshot_sppo_veiculo_dia.sql`
- `veiculo/snapshot_veiculo_licenciamento_dia.sql`
- `cadastro/snapshot_autuacao_disciplinar_historico.sql`

---

## 2. Mudanças relevantes para a apuração de subsídio

Estas são as PRs do upstream que tocam diretamente em lógica de subsídio
(ordenado do mais recente para o mais antigo):

| PR | Modelo / efeito |
|----|-----------------|
| **#189** | Atualiza cálculo do `feed_end_date` em `feed_info_gtfs` + hot-fix em `subsidio_data_versao_efetiva` |
| **#188** | `[HOTFIX]` Refatora `subsidio_data_versao_efetiva` |
| **#195** | Migra e refatora `view_viagem_climatizacao`; exceção em `veiculo_dia` para tratamento de licenciamento |
| **#175** | `[HOTFIX]` Atualiza data de processamento em `veiculo_dia` |
| **#161** | Adiciona exceção no período de processamento de `veiculo_dia` |
| **#145** | Ponto facultativo 24/05/2026 (afeta calendário/apuração) |

Comparado com o que tracei na consulta anterior contra
`prefeitura-rio/pipelines_rj_smtr` (PRs #1410, #1446, #1459), o upstream v3 já
absorveu pelo menos parte dessas correções via #188/#189. Falta confirmar — após
o sync — se PRs como #1410 (reordenação do CASE em `viagens_remuneradas_v2`) e
#1446 (excepcionalidade abril Q2) também já foram portadas.

---

## 3. Infraestrutura / commons

Mudanças em `pipelines/common/` (+532 / −142 linhas, 17 arquivos):

- `capture/gps/` — `constants.py` (+72), `tasks.py` reescrita, `utils.py` novo
- `capture/veiculo/` — `tasks.py` ajustes, `utils.py` novo
- `capture/jae/constants.py` — alteração de IPs dos bancos (#177)
- `treatment/default_treatment/utils.py` — refatoração
- `utils/extractors/` — `ftp.py` refeito, novo `gdrive.py`, `api.py` ajustes
- `utils/implicit_ftp.py` → **renomeado** para `utils/ftp.py`
- Novo decorator de flow com timeout default (#160)

---

## 4. Hotfixes não relacionados a subsídio (relevantes)

- **#191** Coluna `modo` em `transacao_riocard`
- **#187** Join com tabela `cliente` na query de transação
- **#186** Flows de freshness
- **#177** IPs dos bancos `principal_db` e `transacao_db` da Jaé
- **#176** `get_previnity_date_range`
- **#172** Teste lacre
- **#171** Monitoramento temperatura
- **#168** Timeout do `integration__upload_transacao_cct` + teste `test_sincronizacao_tabelas`
- **#164** Adiciona `treatment__operadora_cnpj` no Dockerfile dos flows de cadastro
- **#158** Join nas tabelas de operadoras

---

## 5. Arquivos de orquestração / deploy

- `MIGRATION_PLAN.md` enxugou bastante (−166 / +44 linhas) — reflete pipelines já concluídos
- `queries/CHANGELOG.md` upstream está em **1.1.3 (2026-05-06)** — adiciona `gps_15_minutos_onibus_sppo` em `sources.yml`
- Workflows de CD reformulados (deploy de automations sem duplicação, #150-#155)
- `queries/selectors.yml` (+51 / −0 linhas) — novos selectors para os pipelines migrados

---

## 6. Recomendações pós-sync

1. **Conferir se as PRs do repo antigo já foram replicadas no v3** — especialmente:
   - #1410 (reordenação CASE em `viagens_remuneradas_v2`)
   - #1446 (excepcionalidade abril Q2)
   - Comparar `queries/models/dashboard_subsidio_sppo/staging/viagens_remuneradas_v2.sql` (upstream v3) com o equivalente no antigo.
2. O `treatment__subsidio_sppo_apuracao` ainda **não foi migrado** — quando for, o flow precisará incorporar a versão V8/V9/V14 com toda a lógica do antigo (`pipelines/migration/projeto_subsidio_sppo/flows.py`).
3. Rodar `uv sync --all-packages` após o pull — `uv.lock` mudou (+288 linhas).
4. Validar `dbt parse` / `dbt deps` após o merge — vários selectors e modelos novos.
