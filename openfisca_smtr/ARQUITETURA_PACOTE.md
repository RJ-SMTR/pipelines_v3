# Arquitetura do pacote `openfisca_smtr`

Documento **somente** sobre o OpenFisca. Implementação do zero:
[PLANO_IMPLEMENTACAO.md](PLANO_IMPLEMENTACAO.md). Cliente dbt/DAG:
[MODELOS_DBT_POS_OF.md](MODELOS_DBT_POS_OF.md) (preferir a
[arquitetura_funcoes.md](arquitetura_funcoes.md) parcial).

---

## 1. Objetivo

Regras contratuais **no grão viagem** (I.8 + params I.9), simuláveis sem dbt.

1. `apurar` devolve **só `viagens`** com valores OF.
2. **Frota diária** no OF: `frota_operante = MAX(pico manhã, pico tarde)` por **lote×data** (0 se não dia útil). **FCF contratual** (média quinzenal / estimada) fica no **dbt**.
3. **OPEX por viagem** no OF: `TR × β × km_remuneravel × ipa`. CAPEX / QR / RQ no dbt.
4. **Anexo I.2** → tabelas materializadas + join **antes** do OF (não YAML).
5. Tab. 2 via bools `indicador_viagem_completa`, `indicador_viagem_valida` e `indicador_viagem_conforme`.

---

## 2. Fronteira

| Dentro do OF | Fora (dbt / BQ) |
|--------------|-----------------|
| YAML I.8 (Tab. 2, picos) e I.9 (TR, α, β) | Materializar I.2; classificar; **join** |
| Tab. 2, picos, frota diária, IPA/precária, OPEX viagem | FCF quinzena; CAPEX; QR; RQ; subsídio |
| `apurar` → `viagens` | Histórico, diff, pagamento |

```
I.2 materializado ──┐
                     ├── join ──► rows ──► apurar ──► viagens[] ──► dbt
viagens classificadas┘
```

---

## 3. Fluxo

```
  [dbt] tabelas I.2 + viagem_classificacao_validacao
           │ join
           ▼
  viagens[] (bools Tab.2, id_veiculo, lote, chaves faixa, …)
           │
           ▼
  openfisca_smtr.apurar
    · period = date(datetime_partida) | group-by dia
    · Tab. 2 + frota/pico (YAML I.8)
    · frota_operante ← MAX(count completa+pico_m, completa+pico_t) no lote×data
      (0 se não dia útil); auditoria: frota_pico_manha / frota_pico_tarde
    · IPA/precária por faixa → broadcast
    · remuneracao_opex_viagem ← TR × β × km_remuneravel × ipa
    · TR/α/β (YAML I.9 ← lote)
    · saída: viagens[] + versao_regra + periods
           │
           ▼
  [dbt] FCF quinzena, CAPEX, QR, RQ, subsídio
```

**I.2 não é `parameters/` no OpenFisca.**  
**Fora do OF:** IDT/PRD; FCF contratual; CAPEX/QR/RQ/subsídio.

### Period

`DAY`, `YYYY-MM-DD`, de `datetime_partida` (hora/dow sempre derivados); multi-dia = N simulações.

### Pipeline interno OF

```
(1) Tab. 2 (bools) → indicadores, km_remuneravel
(2) Frota/pico → flags
(3) frota_operante = MAX(pico_m, pico_t) por lote×data; 0 se não dia útil
(4) IPA/precária por faixa → broadcast
(5) remuneracao_opex_viagem = TR × β × km × ipa
(6) TR, α, β ← parametros_lote[lote]
(7) retorno só viagens
```

### YAML no pacote

| Pasta | Origem | Papel |
|-------|--------|--------|
| `impacto_tipo_viagem/` | I.8 Tab. 2 | Indicadores |
| `frota/` | I.8 item 4 | Picos / dia útil |
| `parametros_lote/{A2,B2}/` | I.9 | TR, α, β |

TR: B2 11,53 (0,22/0,78); A2 9,94 (0,24/0,76); B1 deserto.

---

## 4. API (`apurar`) — entrada e saída

```python
resultado = apurar(viagens=[...], id_execucao="...")
# resultado["viagens"]  — único grão
```

Detalhe normativo completo: [PLANO_IMPLEMENTACAO.md §5](PLANO_IMPLEMENTACAO.md).

### 4.1 Entrada — obrigatório (OF)

| Campo | Tipo | Descrição resumida |
|-------|------|--------------------|
| `id_apuracao` | STRING | Chave única da entidade na simulação |
| `datetime_partida` | DATETIME | **Obrigatório** — period + deriva hora/dow |
| `indicador_viagem_completa` | BOOL | Completa vs incompleta (Tab. 2) |
| `indicador_viagem_valida` | BOOL | Válida vs inválida (Tab. 2) |
| `indicador_viagem_conforme` | BOOL | Conforme vs não conforme; exige `valida` |
| `km_programada` | FLOAT | Km do plano — completa que paga km |
| `km_percorrida` | FLOAT | Km reconhecida — incompleta que paga km |
| `lote` | STRING | `A2` / `B2` |
| `id_veiculo` | STRING | Numerador da frota diária (count no OF) |
| `servico`, `sentido`, `faixa_horaria_inicio` | STRING | Grão IPA |
| `servico_viagens_programadas` | INT | Denominador IPA |

`hora_partida` / `day_of_week`: **sempre** derivados de `datetime_partida`.  
IPA/precária: agregados no `apurar` por faixa e broadcast na viagem.  
`lote_frota_estimada`: eco I.2 opcional (FCF quinzena no dbt).

### 4.2 Entrada — recomendado (eco / dbt)

| Campo | Descrição resumida |
|-------|--------------------|
| `id_viagem` | Chave de negócio |
| `placa`, `datetime_chegada` | Auditoria |
| `lote_frota_estimada` | Denominador FCF quinzena (dbt) |

### 4.3 Entrada — I.2 adicional (eco)

| Campo | Descrição resumida |
|-------|--------------------|
| `servico_tipo` | Regular, noturno, … |
| `servico_tecnologia` | Ex. básico, MIDI |
| `lote_frota_determinada` | Teto de frota |
| `lote_qr_mensal` / `lote_km_referencia` | Base CAPEX RQ no dbt |
| `tipo_dia` | Oferta no join |

`rede` **não** entra. Prefixo `servico_` / `lote_` = atributo daquele grão.

**Não enviar:** `frota_operante*`, TR/α/β, `idt`, `lotes[]`, enums antigos de tipo/classificação.

### 4.4 Saída — envelope

`periods`, `period`, `versao_regra`, `id_execucao?`, **`viagens`**.

### 4.5 Saída — por viagem

| Calculado OF | Tipo |
|--------------|------|
| `period` / `data` | STRING / DATE |
| `indicador_quilometragem_pagamento` | BOOL |
| `indicador_percentual_atendimento` | BOOL |
| `km_remuneravel` | FLOAT |
| `indicador_pico_manha` / `pico_tarde` | BOOL |
| `indicador_completa_pico_manha` / `tarde` | BOOL |
| `indicador_dia_util` | BOOL |
| `frota_operante`, `frota_pico_manha`, `frota_pico_tarde` | FLOAT |
| `ipa`, `percentual_atendimento`, `desconto_operacao_precaria`, … | FLOAT |
| `remuneracao_opex_viagem`, `km_ponderada_ipa_viagem` | FLOAT |
| `tarifa_remuneracao`, `alpha`, `beta` | FLOAT (I.9) |

+ eco de todos os fatos de entrada (incl. I.2).

Sem `fcf` contratual / `remuneracao_servico` / `subsidio_*` no OF.

### 4.6 CLI

```bash
python -m openfisca_smtr apurar --viagens viagens.json
```

---

## 5. Operação

| Momento | Fluxo |
|---------|--------|
| Dia | Join I.2 + classificadas → `apurar` → grava |
| Reprocesso | Remanda rows → `apurar` → dbt reaggrega |
| Diff | Cliente, sobre tabelas dbt |

---

## 6. Código (alvo)

```
openfisca_smtr/
  variables/   impacto_viagem, frota, ipa, remuneracao (TR/α/β + frota diaria)
  parameters/  impacto_tipo_viagem, frota, parametros_lote
```

Sem `lote_servico/` / `operacao_lote/` no pacote.

---

## 7. dbt (encaixe)

```
materializar I.2
  → classificar / join (bools + id_veiculo + chaves faixa)
  → apurar → viagens_apuradas (c/ frota_operante, opex, ipa)
  → SQL FCF quinzena / CAPEX / RQ / subsídio
```

---

## 8. Decisões / planejamento

| Tema | Decisão |
|------|---------|
| Saída OF | Só `viagens` |
| I.2 | Tabelas materializadas + join (não YAML OF) |
| Frota diária | MAX(picos) por lote×data; 0 se não dia útil |
| FCF contratual | **dbt** quinzena |
| OPEX | **OF** por viagem (`TR×β×km×ipa`) |
| Tab. 2 | Bools completa / valida / conforme |
| I.9 TR/α/β | YAML OF |
| IDT/PRD | Fora de escopo (`prd=0`) |
