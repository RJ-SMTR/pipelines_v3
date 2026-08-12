# Plano de implementação — `openfisca_smtr`

Guia **para construir o pacote do zero**. Descreve o que implementar, em que
ordem, e com quais contratos. Não descreve estado parcial do repositório.

Contrato detalhado dbt/DAG (cliente): [arquitetura_funcoes.md](arquitetura_funcoes.md).  
Notas de evolução / histórico: [ARQUITETURA_PACOTE.md](ARQUITETURA_PACOTE.md).

---

## 1. O que construir

Pacote Python **OpenFisca** com regras **no grão viagem**, usável **sem dbt**:

```text
viagens classificadas
  + tabelas I.2 materializadas (join no dbt)
       →  rows enriquecidas
       →  apurar(...)
       →  { viagens }          # grão viagem + valores OF (frota diaria, IPA, OPEX)
       →  dbt: FCF quinzena, CAPEX, QR, RQ, …
```



### Fora de escopo do pacote OpenFisca


| Item                                                     | Onde fica                                 |
| -------------------------------------------------------- | ----------------------------------------- |
| Tabelas Anexo I.2 (serviço↔lote, frota plano, oferta, …) | **dbt** — tabelas materializadas + join   |
| FCF contratual (média quinzenal / estimada)              | dbt                                       |
| CAPEX, QR, RQ, subsídio, compensação                     | dbt                                       |
| IDT → PRD (I.8 item 7)                                   | Adiado (`prd=0`)                          |
| Histórico / diff / pagamento                             | Cliente (BQ)                              |
| Classificar viagem                                       | Modelo dbt prévio (fato)                  |


---



## 2. Fronteira


| Camada                 | Responsabilidade                                                              |
| ---------------------- | ----------------------------------------------------------------------------- |
| **dbt (antes do OF)**  | Materializa I.2; classifica; join (`lote`, chaves faixa, …)                   |
| **OpenFisca**          | Tab. 2, picos, frota diária, IPA/precária, **OPEX viagem**; saída só `viagens` |
| **dbt (depois do OF)** | FCF quinzena, CAPEX, QR, RQ, rateio, subsídio; histórico                      |


**Fato** = o que chega na row (operacional **e** planejamento já joinado).  
**Parâmetro OF** = norma tipicamente I.8 (Tab. 2, picos) e I.9 (TR, α, β) em YAML.


| Na row de entrada (após join dbt) | OF calcula                                         |
| --------------------------------- | -------------------------------------------------- |
| `lote`, campos I.2 (eco)          | — (já fatos); TR/α/β ← YAML I.9                    |
| tipo, classificação, km           | indicadores Tab. 2, `km_remuneravel`               |
| `datetime_partida`                | `period` + hora/dow → flags pico / dia útil        |
| `id_veiculo` + flags pico         | `frota_operante` = MAX(picos) por lote×data        |
| chaves faixa + programadas        | IPA / precária (broadcast) + `remuneracao_opex_viagem` |


**I.2 não entra como** `parameters/` **YAML no OpenFisca.**

---



## 3. Period OpenFisca


| Regra               | Valor                                       |
| ------------------- | ------------------------------------------- |
| `definition_period` | `DAY`                                       |
| Formato             | `YYYY-MM-DD`                                |
| Origem              | `date(datetime_partida)`                    |
| Multi-dia           | Agrupar por dia → N simulações → concatenar |


---



## 4. Ordem de implementação



### Fase A — Esqueleto

1. `pyproject.toml` (Python 3.13, `openfisca-core`).
2. `entities.py` — `apuracao` (= viagem).
3. `enums.py` — `TipoApuracao`, `ClassificacaoValidacao`, `Lote` (`A2`, `B2`).
4. `system.py`, `simulation.py`, `version.py`, CLI `simulate`.



### Fase B — Parâmetros YAML (só norma OF)


| Pasta                               | Anexo      | Uso                             |
| ----------------------------------- | ---------- | ------------------------------- |
| `parameters/impacto_tipo_viagem/`   | I.8 Tab. 2 | Indicadores km / % atendimento  |
| `parameters/frota/`                 | I.8 item 4 | Janelas de pico, dia útil       |
| `parameters/parametros_lote/A2|B2/` | I.9        | TR, α, β (se ecoados na viagem) |


Não criar `lote_servico/` nem `operacao_lote/` no pacote OF — isso é tabela
materializada I.2 no dbt.

Valores I.9 (ex. `2026-08-01`): B2 11,53 / 0,22 / 0,78; A2 9,94 / 0,24 / 0,76.

### Fase C — Variáveis (grão viagem)


| Módulo              | Papel                                                       |
| ------------------- | ----------------------------------------------------------- |
| `impacto_viagem.py` | Tab. 2 → indicadores, `km_remuneravel` |
| `frota.py`          | Picos / dia útil                       |
| `ipa.py`            | IPA / precária (agregação no `apurar`) |
| `remuneracao.py`    | TR/α/β; inputs de frota diária         |


Campos I.2 / serviço na row = **inputs** (fatos do join).

Frota operante diária (Anexo I.8 item 4) — **no OF** (`apurar`); FCF contratual na **quinzena (dbt)**:

```text
1. Por viagem: flags completa+pico (indicador_viagem_completa + hora)
2. Por lote×data: frota_pico_manha / frota_pico_tarde = count distinct id_veiculo
3. frota_operante = MAX(manha, tarde) se dia util; senão 0
4. Broadcast em cada viagem do lote×data
5. dbt: AVG(frota_operante) dias uteis / frota estimada → fcf_quinzena
```



### Fase D — `apurar`

```python
resultado = apurar(viagens=[...], id_execucao=...)
# → { "viagens", "periods", "versao_regra", ... }
```

```text
1. period por viagem; group-by dia
2. Tab.2 (bools) + picos + TR/α/β
3. agregar frota_operante por lote×data (MAX picos; 0 se nao dia util)
4. agregar faixa → IPA / precária; broadcast
5. remuneracao_opex_viagem = TR × β × km_remuneravel × ipa
6. retornar só viagens
```



### Fase E — Cliente dbt

```text
1. Materializar I.2 (serviço↔lote, frota estimada, …)
2. Classificar viagens (bools completa/valida/conforme) + join I.2
3. apurar → viagens (frota diaria, IPA/precária, OPEX)
4. SQL: FCF quinzena → CAPEX/QR/RQ / rateio / subsidio
```

---



## 5. Contratos de I/O (detalhado)

O cliente (dbt) monta **uma row por viagem** já joinada com I.2 e classificação.
Não há `lotes[]` na fachada.

### 5.1 Entrada — obrigatório para o OpenFisca calcular

Cada campo abaixo deve existir na row (ou ser derivável antes do `apurar`).


| Campo                       | Tipo     | Origem          | Descrição                                                                                                                                                                                                       |
| --------------------------- | -------- | --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id_apuracao`               | STRING   | Cliente         | Identificador único da entidade OpenFisca nesta execução. Pode ser o `id_viagem` ou chave composta (`data|lote|id_veiculo|datetime_partida`).                                                                   |
| `datetime_partida`          | DATETIME | Viagem          | **Obrigatório.** Define o `period` (`YYYY-MM-DD`) e **sempre** deriva `hora_partida` e `day_of_week` no `apurar` (valores enviados pelo cliente são sobrescritos).                                              |
| `data`                      | DATE     | Viagem (eco)    | Opcional; se omitida, preenchida com o period. Não substitui `datetime_partida`.                                                                                                                                |
| `indicador_viagem_completa` | BOOL     | Classificação   | `true` = viagem **completa** (cumpriu o plano de km). `false` = tratada como **incompleta** para Tab. 2 / `km_remuneravel` (usa `km_percorrida`). Substitui o enum `tipo_apuracao` (`completa` / `incompleta`). |
| `indicador_viagem_valida`   | BOOL     | Classificação   | `true` = validação **válida** (conforme ou não conforme). `false` = **inválida**. Distingue inválida de não conforme na Tab. 2 (% atendimento).                                                                 |
| `indicador_viagem_conforme` | BOOL     | Classificação   | `true` = validação **conforme**. `false` = **não conforme** (só faz sentido se `indicador_viagem_valida`). Invariante: `conforme ⇒ valida`. Junto com `valida`, substitui o enum `classificacao_validacao`.     |
| `km_programada`             | FLOAT    | Plano           | Km planejada. Entra em `km_remuneravel` se completa **e** a Tab. 2 paga km. Senão `0`.                                                                                                                          |
| `km_percorrida`             | FLOAT    | Aferido         | Km reconhecida. Entra em `km_remuneravel` se **não** completa **e** a Tab. 2 paga km. Senão `0`.                                                                                                                |
| `lote`                      | STRING   | Join I.2        | `A2` \| `B2`. Chave econômica; TR/α/β; agregação de `frota_operante`. |
| `id_veiculo`                | STRING   | Viagem          | Necessário para o OF calcular `frota_operante` (count distinct por **lote×data** nos picos). |
| `servico`                   | STRING   | Viagem          | Código do serviço — grão IPA. |
| `sentido`                   | STRING   | Viagem          | `I` / `V` / `C` — grão IPA. |
| `faixa_horaria_inicio`      | STRING/TIME | Viagem       | Início da faixa `h` — grão IPA. |
| `servico_viagens_programadas` | INT    | Plano           | Programadas na faixa (denominador IPA). |


**Derivados (não enviar / ignorados na entrada):** `hora_partida` (0–23), `day_of_week` (Spark 1=dom … 7=sáb).

**Sem cálculo parcial:** falta qualquer campo obrigatório → `ValueError`; a execução não simula.

**Mapeamento Tab. 2 (bools de validação):**


| Estado I.8   | `valida` | `conforme` |
| ------------ | -------- | ---------- |
| conforme     | true     | true       |
| não conforme | true     | false      |
| inválida     | false    | false      |


Combina com `indicador_viagem_completa` (completa vs incompleta). A categoria `nao_apurada` do anexo não tem boolean próprio nesta entrega — se precisar, acrescentar depois.

**Não enviar:** `frota_operante` / `frota_operante_media` (calculados no OF).

**Dependências:** period + hora/dow ← `datetime_partida`; Tab. 2 ← bools + km; picos → frota diária; IPA ← faixa + `servico_viagens_programadas`; OPEX ← TR×β×km×ipa.

### 5.2 Entrada — recomendado (eco)

Chaves de faixa já são obrigatórias em §5.1.


| Campo               | Tipo        | Origem   | Descrição                            |
| ------------------- | ----------- | -------- | ------------------------------------ |
| `id_viagem`         | STRING      | Viagem   | Chave de negócio se ≠ `id_apuracao`. |
| `placa`             | STRING      | Cadastro | Placa do veículo.                    |
| `faixa_horaria_fim` | STRING/TIME | Viagem   | Fim da faixa — auditoria.            |
| `datetime_chegada`  | DATETIME    | Viagem   | Chegada — auditoria.                 |




### 5.3 Entrada — planejamento I.2 adicional (join; eco)

Campos de **lote** ou **serviço** vindos das tabelas materializadas (além de `lote`; `lote_frota_estimada` é eco para o FCF quinzena no dbt):


| Campo                                     | Tipo   | Nível       | Descrição                                                           |
| ----------------------------------------- | ------ | ----------- | ------------------------------------------------------------------- |
| `servico_tipo`                            | STRING | serviço     | Tipo do serviço no POR (regular, noturno, …).                       |
| `servico_tecnologia`                      | STRING | serviço     | Tecnologia prevista no plano para o serviço (ex. `basico`, `midi`). |
| `lote_frota_determinada`                  | FLOAT  | lote        | Frota determinada (teto) do lote.                                   |
| `lote_qr_mensal`                          | FLOAT  | lote        | QR mensal (km) do lote.                                             |
| `lote_km_referencia` / `lote_qr_quinzena` | FLOAT  | lote        | Km de referência do período (ex. QR/2) para CAPEX da RQ no dbt.     |
| `tipo_dia`                                | STRING | oferta/lote | Tipo de dia da oferta (útil, sábado, …) usado no join de frota/QR.  |


**Rede:** não entra na row — só vigência das tabelas I.2 por data.

### 5.3.1 Exemplo de entrada (uma viagem, todos os campos)

```json
{
  "id_apuracao": "2026-08-14|B2|ABC1D23|2026-08-14T07:15:00",
  "id_viagem": "viagem-731-i-20260814-0715",
  "datetime_partida": "2026-08-14T07:15:00",
  "data": "2026-08-14",
  "datetime_chegada": "2026-08-14T08:02:00",
  "indicador_viagem_completa": true,
  "indicador_viagem_valida": true,
  "indicador_viagem_conforme": true,
  "km_programada": 12.4,
  "km_percorrida": 0.0,
  "lote": "B2",
  "lote_frota_estimada": 141.0,
  "id_veiculo": "ABC1D23",
  "placa": "ABC1D23",
  "servico": "731",
  "sentido": "I",
  "faixa_horaria_inicio": "05:00",
  "faixa_horaria_fim": "09:00",
  "servico_viagens_programadas": 8,
  "servico_tipo": "regular",
  "servico_tecnologia": "basico",
  "lote_frota_determinada": 160.0,
  "lote_qr_mensal": 772144.2,
  "lote_km_referencia": 386072.1,
  "tipo_dia": "util"
}
```

Ilustrativo B2 / sexta. `hora_partida` e `day_of_week` omitidos — derivados de `datetime_partida` (7 e Spark 6). Sem `frota_operante*` na entrada.

### 5.4 Entrada — não enviar ao OF


| Campo                                      | Motivo                                                                                                         |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------------- |
| `hora_partida`, `day_of_week`              | Sempre derivados de `datetime_partida` (entrada ignorada)                                                      |
| `tipo_apuracao`, `classificacao_validacao` | Substituídos pelos bools `indicador_viagem_completa` / `indicador_viagem_valida` / `indicador_viagem_conforme` |
| `frota_operante`, `frota_operante_media`   | Calculados **no OF**                                                                                           |
| `tarifa_remuneracao`, `alpha`, `beta`      | YAML I.9 (se ecoados, o OF calcula)                                                                            |
| `idt` / PRD                                | Fora de escopo                                                                                                 |
| `rede`                                     | Só vigência I.2 por data                                                                                       |
| JSON `indicadores` da classificação        | Auditoria dbt                                                                                                  |
| `lotes[]`                                  | Removido da fachada                                                                                            |




### 5.5 Saída — envelope

```python
{
  "periods": ["2026-08-14"],       # dias distintos usados
  "period": "2026-08-14",          # str se 1 dia; list se vários
  "versao_regra": "0.1.0",
  "id_execucao": "...",            # se passado na chamada
  "viagens": [ { ... }, ... ],
}
```



### 5.6 Saída — cada item de `viagens`

**Ecoados** (cópia da entrada, para o dbt não perder contexto): todos os campos
das seções 5.1–5.3 que o cliente enviou.

**Calculados pelo OpenFisca:**


| Campo                               | Tipo        | Significado                                                                  |
| ----------------------------------- | ----------- | ---------------------------------------------------------------------------- |
| `period`                            | STRING      | `YYYY-MM-DD` da simulação desta row                                          |
| `data`                              | DATE/STRING | Preenchido se não veio na entrada (= period)                                 |
| `indicador_quilometragem_pagamento` | BOOL        | Tab. 2 — conta para km de pagamento                                          |
| `indicador_percentual_atendimento`  | BOOL        | Tab. 2 — conta para % atendimento                                            |
| `km_remuneravel`                    | FLOAT       | Km da viagem para QC (programada ou percorrida, ou 0)                        |
| `indicador_pico_manha`              | BOOL        | Partida no pico manhã (params frota)                                         |
| `indicador_pico_tarde`              | BOOL        | Partida no pico tarde                                                        |
| `indicador_completa_pico_manha`     | BOOL        | Completa ∧ pico manhã                                                        |
| `indicador_completa_pico_tarde`     | BOOL        | Completa ∧ pico tarde                                                        |
| `indicador_dia_util`                | BOOL        | Dia útil (params frota / dayofweek)                                          |
| `frota_operante`                    | INT/FLOAT   | MAX(pico_m, pico_t) por lote×data; 0 se não dia útil (OF) |
| `frota_pico_manha` / `frota_pico_tarde` | FLOAT   | Count distinct veículos completa+pico (auditoria)         |
| `tarifa_remuneracao`                | FLOAT       | TR I.9 do `lote`                                          |
| `alpha`                             | FLOAT       | % CAPEX I.9                                               |
| `beta`                              | FLOAT       | % OPEX I.9                                                |
| `viagens_atendimento_faixa`         | INT         | Contagem na faixa (broadcast)                             |
| `viagens_programadas_faixa`         | INT         | Programadas da faixa                                      |
| `percentual_atendimento`            | FLOAT       | atendimento / programadas (faixa)                         |
| `ipa`                               | FLOAT       | 1.0 / 0.9 / 0.6 / 0.0 (faixa, broadcast)                  |
| `desconto_operacao_precaria`        | FLOAT       | 0 / 600 / 1200 (faixa; somar distinct no dbt)             |
| `qc_km_faixa`                       | FLOAT       | Σ km_remuneravel da faixa                                 |
| `qc_km_ponderada_ipa`               | FLOAT       | `qc_km_faixa * ipa`                                       |
| `km_ponderada_ipa_viagem`           | FLOAT       | `km_remuneravel * ipa`                                    |
| `remuneracao_opex_viagem`           | FLOAT       | `TR * β * km_remuneravel * ipa` (`prd=0`)                 |


Não saem do OF: `fcf` contratual, `remuneracao_servico`, `subsidio_*`, CAPEX/QR/RQ (dbt).

### 5.7 Depois do OF (dbt) — referência

Ver [MODELOS_DBT_POS_OF.md](MODELOS_DBT_POS_OF.md). A partir de `viagens_apuradas`:

1. `fcf_quinzena_lote` — AVG(`frota_operante`) dias úteis / estimada (`lote_frota_estimada` eco I.2).
2. CAPEX + QR + RQ / rateio / subsídio (somar IPA/precária **distinct por faixa**).
3. Histórico / `id_execucao` / diff.

---



## 6. Estrutura de pastas (pacote OF)

```text
openfisca_smtr/
  apurar.py, simulation.py, system.py, …
  variables/   impacto_viagem, frota, ipa, remuneracao
  parameters/  impacto_tipo_viagem, frota, ipa, operacao_precaria, parametros_lote
  tests/
```

I.2 = modelos dbt (`queries/…`), não pastas em `parameters/`.

---



## 7. Critérios de pronto

- [ ] `apurar` → só `viagens` + metadados
- [ ] Period diário / multi-dia
- [ ] Bools `indicador_viagem_completa` / `indicador_viagem_valida` / `indicador_viagem_conforme` (Tab. 2)
- [ ] `frota_operante` = MAX(picos) por lote×data (0 se não dia útil); FCF quinzena no dbt
- [ ] `remuneracao_opex_viagem` no OF
- [ ] Nomes `servico_*` / `lote_*` para atributos de serviço/lote
- [ ] I.2 fora do YAML OF (documentado + fluxo dbt join)
- [ ] CLI + pacote sem dbt nas deps

---

