# Contrato de colunas — funções dbt Python (Anexo I.8)

Arquitetura planejada: **dbt classifica e agrega**; **OpenFisca calcula consequências contratuais**.

## Princípio: simulação independente do dbt

A simulação OpenFisca é o **produto principal** do pacote `openfisca_smtr`.
O dbt Python é apenas **um consumidor** (produção em BigQuery), não a única forma de executar as regras.

| Camada | Papel | Depende de dbt? |
|--------|--------|-----------------|
| `openfisca_smtr` (`variables`, `parameters`, `simulate`) | Regras versionadas + API de simulação | **Não** |
| CLI `python -m openfisca_smtr` | Rodar fatos JSON → resultados JSON | **Não** |
| Testes `pytest openfisca_smtr/tests` | Validar regras | **Não** |
| Modelos dbt Python (laranja) | Ler fatos BQ → chamar `simulate` → gravar BQ | Sim (só orquestração) |

Contrato da API standalone:

```python
from openfisca_smtr import simulate

resultado = simulate(
    rows=[{"id_apuracao": "v1", "tipo_apuracao": "completa", "hora_partida": 7, "day_of_week": 2}],
    period="2026-08",
    outputs=["indicador_completa_pico_manha", "indicador_dia_util"],
)
```

CLI:

```bash
python -m openfisca_smtr --period 2026-08 \
  --output indicador_completa_pico_manha \
  --input fatos.json
```

Fluxo alvo do processo completo (mesma API, com ou sem dbt):

```
fatos (CSV/JSON/BQ) ──► openfisca_smtr.simulate ──► resultados
                              ▲
                              │
                    dbt Python (opcional) só I/O BigQuery + agregações
```

## Princípio de grão (obrigatório)

| Uso | Grão | Motivo |
|-----|------|--------|
| **Apuração / verificação** | **viagem** (`id_viagem`) | Auditar cada valor (km, indicadores, parcela RQ) |
| **Pagamento** | **veículo** (`id_veiculo` × quinzena × lote) | Repasse operacional por veículo |
| **Fórmula contratual** | lote × quinzena | Anexo I.8 item 3 (RQ, FCF, PRD, RT) |
| **IPA / precária** | serviço × sentido × faixa `h` | Anexo I.8 item 6 |

Fluxo de valores:

```
viagem  ──(OF)──► km_remuneravel, indicadores
    │
    ├─ agrega ──► faixa (IPA) ──► lote×quinzena (RQ, S)
    │                                    │
    └─ rateio ◄──── parcela da RQ ───────┘
            │
            ▼
     remuneracao_viagem   (verificação)
            │
            ▼ SUM por id_veiculo
     remuneracao_veiculo_quinzena   (pagamento)
```

A entidade OpenFisca `apuracao` deve ser instanciada **por viagem** na etapa de verificação. Fatores de lote/faixa (`fcf`, `ipa`, `prd`, `tr`, `alpha`, `beta`) entram como atributos repetidos em cada viagem daquele contexto (broadcast), para o valor da viagem ser reproduzível isoladamente.

Legenda do diagrama:

| Cor | Papel |
|-----|--------|
| Amarelo | **Parâmetros OpenFisca** (`openfisca_smtr/parameters/`) — valores de I.2 e I.9 |
| Azul | Tabelas pré-existentes (fatos) |
| Verde | Tabelas novas (fatos / staging) |
| Roxo | OpenFisca como sistema (`variables.py` + `parameters/*.yaml`) |
| Laranja | Funções dbt Python (orquestram fatos → simulação OF → saídas) |

Ou seja: **amarelo ⊂ roxo**. Os blocos amarelos não são seeds dbt à parte — são o *conteúdo paramétrico* do OpenFisca. O dbt só passa fatos (viagem, bilhetagem, frota operante aferida); TR/α/β/QR/frota estimada/serviço↔lote entram via `parameters(period)`.

Convenções:

- Datas: `data` (DATE); vigência de parâmetro OF por data no YAML (`2026-08-01:` etc.).
- Quinzena: `ano`, `mes`, `quinzena` ∈ {1, 2} (1 = dias 1–15; 2 = 16–fim).
- Sentido: `I` / `V` / `C` (padrão do projeto).
- Enums OpenFisca: `tipo_apuracao` ∈ {completa, incompleta, nao_apurada}; `classificacao_validacao` ∈ {conforme, nao_conforme, invalida}.
- Nomes de saída alinhados a `openfisca_smtr/variables.py` quando houver correspondente.
- **`id_veiculo` sobe em toda a cadeia de apuração** (nunca descartar antes do pagamento).

---

## Parâmetros amarelos = parâmetros OpenFisca (fontes I.2 e I.9)

Os três blocos amarelos são **parâmetros versionados do TaxBenefitSystem**, não variáveis de input nem tabelas dbt.

| Bloco (diagrama) | Pacote OF sugerido | Anexo-fonte | Papel |
|------------------|--------------------|-------------|--------|
| `lote_servico` | `parameters/lote_servico/` | **I.2** | Mapa serviço → lote |
| `operacao_lote` | `parameters/operacao_lote/` | **I.2** (+ I.9 Tab. 3) | Frota estimada, QR, tipo_dia |
| `parametros_lote` | `parameters/parametros_lote/` | **I.9** | TR, α, β, tarifa pública |

Já existentes no pacote (regras I.8, também roxo):

| Pacote OF | Anexo | Papel |
|-----------|-------|--------|
| `parameters/impacto_tipo_viagem/` | I.8 Tab. 2 | Impacto km / atendimento |
| `parameters/ipa.yaml` | I.8 §6.1 | Faixas IPA |
| `parameters/operacao_precaria/` | I.8 §6.2 | Descontos R$ |
| `parameters/prd.yaml` | I.8 §7 | Faixas PRD |

> **Estado atual:** TR/α/β em `parameters/parametros_lote/{A2,B2}/` (YAML).
> Fórmulas em `variables/remuneracao.py` leem `parameters(period).parametros_lote…`
> via enum `Lote`. **Alvo I.2:** `lote_servico` (serviço→lote) e `operacao_lote`
> (QR, frota estimada) em YAML — o cliente **não** envia `lote` nem a tabela de
> planejamento; envia `servico` + fatos aferidos. Hoje `lote` e `km_referencia`
> ainda são inputs (transição).

### `lote_servico` ← Anexo I.2 → `parameters/lote_servico/`

Derivado do POR (Tabelas 3–23 do I.2). Em OpenFisca: parâmetro indexado por `servico` (e opcionalmente rede/tipo_dia), retornando `lote`.

| Campo | Tipo | Origem I.2 |
|-------|------|------------|
| `servico` | chave | Numeral (ex.: `731`, `SN790`) |
| `lote` | STRING | `B1` / `B2` / `A2` |
| `rede` | STRING | `entrada` / `plena` / `plena_expandida` |
| `vista` | STRING | Vista |
| `tipo_servico` | STRING | Regular, Noturno, … |
| `tipo_veiculo` | STRING | Básico / MIDI / Mini |

Uso nas funções laranja: resolver `lote` da viagem antes/durante a simulação OF.

### `operacao_lote` ← Anexo I.2 / I.9 → `parameters/operacao_lote/`

| Campo | Tipo | Origem | Uso na fórmula |
|-------|------|--------|----------------|
| `lote` | chave | I.2 / I.9 | — |
| `tipo_dia` | chave | I.2 | Oferta / FCF |
| `rede` | chave | I.2 | POR vigente |
| `frota_estimada` | FLOAT | I.2 POR; I.9 Tab.3 frota operante | Denominador FCF |
| `frota_determinada` | FLOAT | I.2 Tab.24–26 / I.9 Tab.3 | Teto (I.8 item 4) |
| `qr_mensal` | FLOAT | I.2 Σ produção km; I.9 Tab.3 | `km_referencia` = qr/2 |

Baseline I.9 Tab. 3:

| Lote | Frota determinada | Frota operante | QR mensal (km) |
|------|-------------------:|---------------:|---------------:|
| B1 | 152 | 135 | 1.085.885,1 |
| B2 | 160 | 141 | 772.144,2 |
| A2 | 216 | 190 | 1.342.970,3 |

### `parametros_lote` ← Anexo I.9 → `parameters/parametros_lote/`

| Campo | Tipo | Origem I.9 | Lê em `variables.py` |
|-------|------|------------|----------------------|
| `lote` | chave | B1 / B2 / A2 | — |
| `tr` | FLOAT | Tarifa de Referência R$/km: **B2 11,53**; **A2 9,94** (B1 deserto) | `tarifa_remuneracao` ← YAML |
| `alpha` | FLOAT | % CAPEX: B2 22%; A2 24% | `alpha` ← YAML |
| `beta` | FLOAT | % OPEX: B2 78%; A2 76%; α+β=1 | `beta` ← YAML |
| `tarifa_publica` | FLOAT | Decreto / I.9 §6.1 (`fa_publica` no diagrama) | auditoria / bilhetagem |

### Como o dbt Python usa

```
fatos (azul/verde)  +  parameters/*.yaml (amarelo + regras I.8)
                         │
                         ▼
              TaxBenefitSystem / Simulation
                         │
                         ▼
              variáveis calculadas → tabelas laranja
```

- **Não** duplicar TR/α/β/QR em tabela BigQuery “parametros_lote” como fonte de verdade.
- Se precisar expor no BQ para auditoria, materializar um *snapshot* dos parâmetros OF da quinzena, gerado a partir do mesmo YAML.

---

## 0. Pré-requisito (verde): `viagem_classificacao_validacao`

Não é função laranja; é a tabela nova que **classifica cada viagem** (completa / válida / conforme) e alimenta as `aux_*`.

**Responsabilidade central:** determinar, por viagem, se é:

| Dimensão (negócio) | Coluna | Valores |
|--------------------|--------|---------|
| **Completa** (apuração) | `tipo_apuracao` | `completa` / `incompleta` / `nao_apurada` |
| **Válida** / **conforme** (validação I.8 §5) | `classificacao_validacao` | `conforme` / `nao_conforme` / `invalida` |

Além disso, deve carregar **todos os indicadores e motivos de classificação** no mesmo padrão de `viagem_classificada` (JSON `indicadores` com `valor` + metadados), **incluindo temperatura e viagem sem transação**. Origens dos dados: a definir depois.

**Grão:** 1 linha = 1 viagem apurada.

### Output — identidade e contexto

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `data` | DATE | Dia (partição) |
| `id_viagem` | STRING | Chave da viagem |
| `id_veiculo` | STRING | Veículo |
| `placa` | STRING | Placa |
| `ano_fabricacao` | INT | Ano fabricação |
| `modo` | STRING | Ex.: Ônibus SPPO |
| `servico` | STRING | Serviço |
| `sentido` | STRING | I/V/C |
| `lote` | STRING | Lote contratual |
| `faixa_horaria_inicio` | DATETIME/TIME | Intervalo `h` |
| `faixa_horaria_fim` | DATETIME/TIME | Fim `h` |
| `datetime_partida` | DATETIME | Partida (também fonte do pico em `aux_frota_*`) |
| `datetime_chegada` | DATETIME | Chegada |
| `distancia_planejada` / `km_programada` | FLOAT | Km do plano (viagem completa) |
| `km_percorrida` | FLOAT | Km percorrida (viagem incompleta) |
| `tecnologia_apurada` | STRING | Tecnologia aferida |
| `tecnologia_remunerada` | STRING | Tecnologia para remuneração |
| `indicador_viagem_autorizada` | BOOL | Autorizada além do plano |

### Output — classificação I.8 (consumo OpenFisca)

| Coluna | Tipo | OpenFisca |
|--------|------|-----------|
| `tipo_apuracao` | STRING | `tipo_apuracao` |
| `classificacao_validacao` | STRING | `classificacao_validacao` |

Deriváveis:

| Coluna | Regra |
|--------|--------|
| `indicador_completa` | `tipo_apuracao = 'completa'` |
| `indicador_valida` | `classificacao_validacao IN ('conforme', 'nao_conforme')` |
| `indicador_conforme` | `classificacao_validacao = 'conforme'` |

### Output — motivo de classificação (como `viagem_classificada`)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `tipo_viagem` | STRING | Motivo/status priorizado (mesmo vocabulário de `viagem_classificada` + extensões temperatura / sem transação) |
| `indicadores` | JSON | Pacote completo de indicadores com motivos (ver abaixo) |

Valores típicos de `tipo_viagem` (herdados / estendidos):

- Veículo: `Não licenciado`, `Não vistoriado`, `Lacrado`, `Não autorizado por ausência de ar-condicionado`, `Não autorizado por capacidade`, `Autuado por ar inoperante`, `Registrado com ar inoperante`, `Licenciado sem ar e não autuado`, `Licenciado com ar e não autuado`
- Autuação na viagem: `Autuado por alterar itinerário`, `Autuado por vista inoperante`, `Autuado por não atender solicitação de parada`, `Autuado por iluminação insuficiente`, `Autuado por não concluir itinerário`
- Temperatura: ex. irregularidade de ar-condicionado / falha recorrente (conforme regra atual de `viagem_regularidade_temperatura`)
- Bilhetagem: `Sem transação` (e correlatos de equipamento)

### Conteúdo de `indicadores` (JSON)

Cada chave segue o padrão `{ "valor": bool|…, …metadados }`.

**1. Já presentes em `viagem_classificada` / veículo-dia**

| Indicador | Papel / motivo |
|-----------|----------------|
| `indicador_licenciado` | Licenciamento |
| `indicador_vistoriado` | Vistoria |
| `indicador_veiculo_lacrado` | Lacre |
| `indicador_ar_condicionado` | Veículo com ar |
| `indicador_autuacao_ar_condicionado` | Autuação ar inoperante |
| `indicador_registro_agente_verao_ar_condicionado` | Registro agente verão |
| `indicador_penalidade_tecnologia` | Capacidade / tecnologia abaixo do mínimo |

**2. Temperatura (além de `viagem_classificada`)**

| Indicador | Papel |
|-----------|--------|
| `indicador_temperatura_transmitida_viagem` | Houve transmissão ≠ 0 |
| `indicador_temperatura_variacao_viagem` | Variação ao longo da viagem |
| `indicador_temperatura_zero_viagem` | Registros zerados |
| `indicador_temperatura_nula_viagem` | Registros nulos |
| `indicador_temperatura_pos_tratamento_descartada_viagem` | Descarte pós-tratamento |
| `indicador_temperatura_regular_viagem` | Temperatura regular |
| `indicador_regularidade_ar_condicionado_viagem` | Regularidade consolidada da viagem |
| `indicador_falha_recorrente` | Falha recorrente do veículo (com `data_verificacao_falha`) |

**3. Viagem sem transação (além de `viagem_classificada`)**

| Indicador / campo | Papel |
|--------------------|--------|
| `indicador_sem_transacao` | Sem embarque registrado na viagem |
| `indicador_estado_equipamento_aberto` | Validador aberto / em funcionamento |
| `quantidade_transacao` | Contagem Jaé (metadado numérico) |
| `quantidade_transacao_riocard` | Contagem RioCard |
| `indicador_gps_servico_divergente` | Divergência serviço GPS × bilhetagem (se aplicável) |

**4. Itens I.8 §5 (i)–(vi)** — a mapear nos indicadores acima / novos flags quando necessário

| Item normativo | Relação esperada |
|----------------|------------------|
| (i) validador + vista + GPS | associação / autuações vista / GPS |
| (ii) licenciado, vistoriado, não lacrado, circulação | `indicador_licenciado`, `vistoriado`, `veiculo_lacrado` |
| (iii) ≥1 transação e validador ok | `indicador_sem_transacao`, `indicador_estado_equipamento_aberto` |
| (iv) parada para embarque | autuação “não atender solicitação de parada” |
| (v) tipologia compatível | `indicador_penalidade_tecnologia` / tecnologias |
| (vi) climatização adequada | ar + temperatura + autuações ar |

> Pico (05h–09h / 16h–20h) **não** é coluna desta tabela — derivado de `datetime_partida` em `aux_frota_operante_dia_lote`.
>
> OpenFisca consome só `tipo_apuracao` + `classificacao_validacao` (+ km). O JSON `indicadores` e `tipo_viagem` são para **auditoria / motivo** da classificação.


---

## 1. `aux_frota_operante_dia_lote`

**Papel (Anexo I.8 item 4):** frota operante diária do lote = maior quantitativo entre picos manhã/tarde, em dias úteis, de veículos com ≥1 viagem **completa**.

**Grão:** `data` × `lote`

### Inputs

| Fonte | Colunas |
|-------|---------|
| `viagem_classificacao_validacao` | `data`, `lote`, `id_veiculo`, `tipo_apuracao`, **`datetime_partida`** |
| `lote_servico` (parâmetro OF / I.2) | `servico`, `lote`, … (vigência no YAML) |
| calendário / regra | dia útil (a partir de `data`) |

### Lógica (dbt Python / SQL)

Pico deduzido de `datetime_partida` (hora local America/Sao_Paulo):

```
pico_manha = hora em [05:00, 09:00)
pico_tarde = hora em [16:00, 20:00)

para cada (data, lote) em dia útil:
  frota_pico_manha = COUNT DISTINCT id_veiculo
    WHERE tipo_apuracao = 'completa' AND pico_manha(datetime_partida)
  frota_pico_tarde = COUNT DISTINCT id_veiculo
    WHERE tipo_apuracao = 'completa' AND pico_tarde(datetime_partida)
  frota_operante = MAX(frota_pico_manha, frota_pico_tarde)
```

### Output

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `data` | DATE | Dia |
| `lote` | STRING | Lote |
| `indicador_dia_util` | BOOL | Dia útil (derivado de `data`) |
| `frota_pico_manha` | INT | Veículos distintos com viagem completa partida 05h–09h |
| `frota_pico_tarde` | INT | Veículos distintos com viagem completa partida 16h–20h |
| `frota_operante` | INT | max(manhã, tarde) |

**OpenFisca:** regras de pico / completa / dia util em `parameters/frota/` + variaveis
(`hora_partida`, `indicador_completa_pico_*`, `indicador_dia_util`).
O modelo dbt Python so prepara fatos, chama a simulacao e agrega `countDistinct`.

---

## 2. `aux_conformidade_servico_faixa_lote`

**Papel (Tabela 2):** (a) apurar OpenFisca **por viagem** para verificação; (b) agregar no grão da faixa para IPA/QC.

### 2a. Saída por viagem (obrigatória — verificação)

**Grão:** `id_viagem`

Chamada OpenFisca **por viagem**:

| Input OF | Output OF |
|----------|-----------|
| `tipo_apuracao`, `classificacao_validacao`, `km_programada`, `km_percorrida` | `indicador_quilometragem_pagamento`, `indicador_percentual_atendimento`, `km_remuneravel` |

| Coluna | Tipo | Descrição | OpenFisca |
|--------|------|-----------|-----------|
| `id_viagem` | STRING | Viagem | entidade `apuracao` |
| `id_veiculo` | STRING | Veículo (pagamento) | — |
| `data`, `lote`, `servico`, `sentido` | — | Contexto | — |
| `faixa_horaria_inicio`, `faixa_horaria_fim` | — | Intervalo `h` | — |
| `tipo_apuracao` | STRING | completa / incompleta / nao_apurada | `tipo_apuracao` |
| `classificacao_validacao` | STRING | conforme / nao_conforme / invalida | `classificacao_validacao` |
| `km_programada` | FLOAT | Km plano (completa) | `km_programada` |
| `km_percorrida` | FLOAT | Km percorrida (incompleta) | `km_percorrida` |
| `indicador_quilometragem_pagamento` | BOOL | Tab. 2 | `indicador_quilometragem_pagamento` |
| `indicador_percentual_atendimento` | BOOL | Tab. 2 | `indicador_percentual_atendimento` |
| `km_remuneravel` | FLOAT | QC da viagem | `km_remuneravel` |

Materialização sugerida: tabela `aux_conformidade_viagem` (ou mesma função com duas saídas / modelo intermediário).

### 2b. Agregação por faixa (IPA)

**Grão:** `data` × `lote` × `servico` × `sentido` × `faixa_horaria_inicio`

```
viagens_programadas = do plano (teto + autorizadas)
viagens_atendimento = COUNT WHERE indicador_percentual_atendimento
qc_km               = SUM(km_remuneravel)
```

| Coluna | Tipo | Descrição | OpenFisca |
|--------|------|-----------|-----------|
| chaves da faixa | — | data, lote, servico, sentido, faixa | — |
| `viagens_programadas` | INT | Programadas (teto) | `viagens_programadas` |
| `viagens_atendimento` | INT | Contam no % atendimento | `viagens_atendimento` |
| `viagens_pagamento` | INT | Contam em km pagamento | — |
| `qc_km` | FLOAT | Σ km remunerável (QC) | alimenta `km_ponderada_ipa` |
| `km_programada_faixa` | FLOAT | Km planejada da faixa | — |
| `qtd_veiculos_faixa` | INT | DISTINCT `id_veiculo` (auditoria) | — |

---

## 3. `fcf_quinzena_lote`

**Papel (item 4):** FCF = min(1, frota_operante_média / frota_estimada).

**Grão:** `ano` × `mes` × `quinzena` × `lote`

### Inputs

| Fonte | Colunas |
|-------|---------|
| `aux_frota_operante_dia_lote` | `data`, `lote`, `frota_operante`, `indicador_dia_util` |
| `operacao_lote` (parâmetro OF / I.2–I.9) | `frota_estimada`, `tipo_dia`, `qr_mensal`, … |
| OpenFisca / regra | teto FCF = 1.0 (hoje `fcf` é input; cap pode ficar no dbt ou virar parâmetro YAML) |

### Lógica

```
frota_operante_media = AVG(frota_operante) nos dias úteis da quinzena
fcf = MIN(1.0, frota_operante_media / frota_estimada)
```

### Output

| Coluna | Tipo | Descrição | OpenFisca |
|--------|------|-----------|-----------|
| `ano` | INT | Ano | — |
| `mes` | INT | Mês | — |
| `quinzena` | INT | 1 ou 2 | — |
| `lote` | STRING | Lote | — |
| `frota_operante_media` | FLOAT | Média diária dias úteis | — |
| `frota_estimada` | FLOAT | Do plano / Anexo I.2 | — |
| `fcf` | FLOAT | ∈ [0, 1] | `fcf` |
| `qr_quinzena` | FLOAT | `qr_mensal / 2` | `km_referencia` |

---

## 4. `ipa_servico_faixa_lote`

**Papel (itens 6.1 e 6.2):** percentual de atendimento → IPA; desconto por operação precária.

**Grão:** `data` × `lote` × `servico` × `sentido` × `faixa_horaria_inicio`  
(agregável depois para quinzena; o PDF apura IPA por serviço/sentido/intervalo `h`)

### Inputs

| Fonte | Colunas |
|-------|---------|
| `aux_conformidade_servico_faixa_lote` | `viagens_programadas`, `viagens_atendimento`, `qc_km`, chaves |
| `operacao_lote` (parâmetro OF) | vigência / tipo_dia |
| flag operacional (opcional) | `indicador_frota_realocada_emergencial` (isenta 6.2) |
| OpenFisca | `percentual_atendimento`, `ipa`, `desconto_operacao_precaria` |

### Chamada OpenFisca

| Input | Output |
|-------|--------|
| `viagens_programadas`, `viagens_atendimento` | `percentual_atendimento`, `ipa`, `desconto_operacao_precaria` |

### Output

| Coluna | Tipo | Descrição | OpenFisca |
|--------|------|-----------|-----------|
| chaves do grão | — | data, lote, servico, sentido, faixa | — |
| `percentual_atendimento` | FLOAT | atendimento / programadas | `percentual_atendimento` |
| `ipa` | FLOAT | 1.0 / 0.9 / 0.6 / 0.0 | `ipa` |
| `desconto_operacao_precaria` | FLOAT | 0 / 600 / 1200 | `desconto_operacao_precaria` |
| `qc_km` | FLOAT | eco da conformidade | — |
| `qc_km_ponderada_ipa` | FLOAT | `qc_km * ipa` | parcela de `km_ponderada_ipa` |

---

## 5. `Remuneracao_quinzena_lote`

**Papel:** (a) calcular RQ contratual no lote×quinzena; (b) **ratear por viagem** para verificação; (c) **agregar por veículo** para pagamento.

### 5a. Total contratual (lote × quinzena)

**Grão:** `ano` × `mes` × `quinzena` × `lote`

| Fonte | Colunas |
|-------|---------|
| `fcf_quinzena_lote` | `fcf`, `qr_quinzena` |
| `ipa_servico_faixa_lote` | Σ `qc_km_ponderada_ipa`, Σ `desconto_operacao_precaria` |
| `parameters/parametros_lote` (OF / I.9) | `tr`, `alpha`, `beta` via `parameters(period)` |
| `Bilhetagem` | `receita_tarifa_publica` (fato) |
| IDT | `idt` → `prd` |
| OpenFisca formulas | `prd`, `remuneracao_servico` |

```
RQ_bruta = TR * (alpha * km_referencia * fcf + beta * km_ponderada_ipa * (1 - prd))
RQ       = RQ_bruta - SUM(desconto_operacao_precaria) [- desconto_evasao]
```

Parcelas para rateio:

```
parcela_capex = TR * alpha * km_referencia * fcf
parcela_opex  = TR * beta * km_ponderada_ipa * (1 - prd)
# descontos: rateio conforme regra de negócio (por faixa que gerou o desconto, ou proporcional ao opex)
```

| Coluna | Tipo | OpenFisca |
|--------|------|-----------|
| chave lote×quinzena | — | — |
| `tarifa_remuneracao`, `alpha`, `beta` | FLOAT | inputs |
| `km_referencia`, `fcf`, `km_ponderada_ipa` | FLOAT | inputs |
| `idt`, `prd` | FLOAT | `idt` / `prd` |
| `desconto_operacao_precaria_total` | FLOAT | — |
| `receita_tarifa_publica` | FLOAT | `receita_tarifa_publica` |
| `remuneracao_servico` | FLOAT | `remuneracao_servico` |
| `parcela_capex`, `parcela_opex` | FLOAT | decomposição |

> Gap: `desconto_operacao_precaria` ainda não abate em `variables.py`.

### 5b. Rateio por viagem (verificação) — `remuneracao_viagem`

**Grão:** `id_viagem`  
**Requisito:** toda linha deve permitir recompor o valor com os fatores broadcast do lote/faixa.

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `id_viagem` | STRING | Chave de verificação |
| `id_veiculo` | STRING | Destino do pagamento |
| `data`, `lote`, `servico`, `sentido`, `faixa_horaria_inicio` | — | Contexto |
| `ano`, `mes`, `quinzena` | — | Quinzena |
| `km_remuneravel` | FLOAT | QC da viagem |
| `ipa` | FLOAT | IPA da faixa (broadcast) |
| `km_ponderada_ipa_viagem` | FLOAT | `km_remuneravel * ipa` |
| `peso_opex_viagem` | FLOAT | `km_ponderada_ipa_viagem / km_ponderada_ipa_lote` (0 se denom=0) |
| `remuneracao_opex_viagem` | FLOAT | `parcela_opex * peso_opex_viagem` |
| `peso_capex_viagem` | FLOAT | regra de rateio CAPEX (ver nota) |
| `remuneracao_capex_viagem` | FLOAT | `parcela_capex * peso_capex_viagem` |
| `desconto_viagem` | FLOAT | parcela dos descontos 6.x |
| `remuneracao_viagem` | FLOAT | capex + opex − desconto |
| `tr`, `alpha`, `beta`, `fcf`, `prd` | FLOAT | fatores broadcast (auditoria) |

**Rateio CAPEX (decisão de negócio — default sugerido):**

- CAPEX é por frota/QR do lote, não por km conforme.
- Default: ratear `parcela_capex` **igualmente entre veículos que entraram no FCF** na quinzena (ou proporcional a dias com frota operante), e dentro do veículo igualmente entre suas viagens completas em pico; **alternativa** mais simples para v1: proporcional a `km_remuneravel` (mesmo peso do OPEX), documentando a diferença.

**Invariante de auditoria:**

```
SUM(remuneracao_viagem) == remuneracao_servico   (tolerância monetária)
SUM(remuneracao_viagem) GROUP BY id_veiculo == remuneracao_veiculo
```

### 5c. Pagamento por veículo — `remuneracao_veiculo_quinzena`

**Grão:** `ano` × `mes` × `quinzena` × `lote` × `id_veiculo`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `ano`, `mes`, `quinzena`, `lote` | — | Chave |
| `id_veiculo` | STRING | Beneficiário do pagamento |
| `qtd_viagens` | INT | Viagens do veículo na quinzena |
| `km_remuneravel` | FLOAT | Σ km |
| `km_ponderada_ipa` | FLOAT | Σ km×IPA |
| `remuneracao_capex` | FLOAT | Σ |
| `remuneracao_opex` | FLOAT | Σ |
| `desconto` | FLOAT | Σ |
| `remuneracao_veiculo` | FLOAT | **valor a pagar (componente RQ)** |
| `indicador_entrou_fcf` | BOOL | Teve viagem completa em pico (auditoria FCF) |

---

## 6. `Subsidio_quinzena_lote`

**Papel (itens 8–9):** subsídio e compensação no lote; espelho rateado para verificação/pagamento por veículo.

### 6a. Total lote × quinzena

**Grão:** `ano` × `mes` × `quinzena` × `lote`

| Input | Output OF |
|-------|-----------|
| `remuneracao_servico`, `receita_tarifa_publica`, `saldo_compensacao_anterior` | `subsidio_bruto`, `subsidio_liquido`, `saldo_compensacao_posterior` |

| Coluna | Tipo | OpenFisca |
|--------|------|-----------|
| chave lote×quinzena | — | — |
| `remuneracao_servico`, `receita_tarifa_publica` | FLOAT | eco |
| `subsidio_bruto` | FLOAT | `subsidio_bruto` |
| `saldo_compensacao_anterior` | FLOAT | input |
| `subsidio_liquido` | FLOAT | `subsidio_liquido` |
| `saldo_compensacao_posterior` | FLOAT | `saldo_compensacao_posterior` |

### 6b. Rateio receita / subsídio por veículo (pagamento)

A RT da bilhetagem é do lote; para pagar por veículo:

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| chave + `id_veiculo` | — | — |
| `receita_tarifa_publica_veiculo` | FLOAT | receita aferida do veículo (bilhetagem por `id_veiculo`) **preferencial**; se indisponível, rateio proporcional a `remuneracao_veiculo` |
| `remuneracao_veiculo` | FLOAT | de 5c |
| `subsidio_bruto_veiculo` | FLOAT | `remuneracao_veiculo - receita_tarifa_publica_veiculo` |
| `subsidio_liquido_veiculo` | FLOAT | após rateio do saldo de compensação do lote (proporcional ao bruto positivo, ou regra CCT) |

**Invariante:**

```
SUM(subsidio_liquido_veiculo) == subsidio_liquido   (lote)
```

Compensação intertemporal (`saldo_*`) permanece **contábil no lote**; o rateio por veículo é só para ordem de pagamento/verificação.

---

## Ordem de execução

```
viagem_classificacao_validacao
        ├─► aux_frota_operante_dia_lote ──► fcf_quinzena_lote ──────────────┐
        └─► aux_conformidade (OF por viagem) ──► ipa_servico_faixa_lote ────┤
                         │                                                    ▼
                         │              parametros + Bilhetagem + IDT + OF
                         │                    Remuneracao_quinzena_lote (total)
                         │                         │
                         └──── rateio ◄────────────┤
                                   │               ▼
                         remuneracao_viagem    Subsidio_quinzena_lote (total)
                                   │               │
                                   ▼               ▼ rateio
                         remuneracao_veiculo   subsidio_veiculo   ← pagamento
```

Dependências:

1. `aux_frota_operante_dia_lote`
2. `aux_conformidade_servico_faixa_lote` (**persiste grão viagem** + agrega faixa)
3. `fcf_quinzena_lote`
4. `ipa_servico_faixa_lote`
5. `Remuneracao_quinzena_lote` (total → rateio viagem → agrega veículo)
6. `Subsidio_quinzena_lote` (total lote → rateio veículo; saldo contábil no lote)

---

## Onde chamar OpenFisca

| Função | Grão OF | Variáveis |
|--------|---------|-----------|
| `aux_conformidade_*` | **viagem** (principal) | Tab.2 + `km_remuneravel` |
| `ipa_servico_faixa_lote` | faixa | `ipa`, `desconto_operacao_precaria` |
| `Remuneracao_quinzena_lote` | lote×quinzena (+ broadcast na viagem) | `prd`, `remuneracao_servico` |
| `Subsidio_quinzena_lote` | lote×quinzena | `subsidio_*`, `saldo_compensacao_*` |
| `aux_frota_*` / `fcf_*` | — | fatos; `fcf` só input |

Para verificação pontual de uma viagem: reexecutar OF com inputs da viagem + fatores broadcast (`ipa`, `fcf`, `prd`, `tr`, `alpha`, `beta`, pesos) e conferir `remuneracao_viagem`.

---

## Lacunas vs Anexo I.8 (próximos incrementos)

| Item | Status no contrato |
|------|--------------------|
| Apuração por viagem + pagamento por veículo | Incorporado (seções 2a, 5b, 5c, 6b) |
| Regra exata de rateio CAPEX | Decisão de negócio pendente (default documentado) |
| Rateio do saldo de compensação entre veículos | Regra CCT pendente |
| 6.3 Evasão tarifária | Colunas futuras; preferir desconto por serviço e rateio às viagens do serviço |
| Abatimento desconto precária na RQ | Gap em `variables.py` |
| Exceção frota realocada (6.2) | Flag em `ipa_servico_faixa_lote` |
| 11 Reajuste TR / 13 Receitas acessórias | Fora do fluxo quinzenal |
