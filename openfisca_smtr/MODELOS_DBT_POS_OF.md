# Modelos dbt posteriores ao OpenFisca

Contrato da cadeia **depois** de `openfisca_smtr.apurar`.  
Dataset dbt: **`sistemario`** / `sistemario_staging` (`queries/models/sistemario/`).  
Fronteira OF: Tab. 2 + picos + `frota_operante` diária (MAX picos, 0 se não dia útil) + IPA/precária + **OPEX por viagem**.

## Fronteira

| Camada | Responsabilidade |
|--------|------------------|
| OpenFisca (`apurar`) | Tab. 2, picos, frota diária lote×data, IPA/precária, `remuneracao_opex_viagem` → só `viagens` |
| dbt | Persistir `viagens_apuradas`; FCF **quinzena**; CAPEX; QR; RQ; rateio; subsídio |

```text
viagem_valida
  → viagem_valida_classificada
  → aux_viagem_valida_temperatura → viagem_valida_regularidade_temperatura
  → viagem_valida_bilhetagem
  → viagem_classificacao_validacao  (+ servico_oferta_faixa, lote_servico)
       → apurar (OF)
       → viagens_apuradas
            ├─► fcf_quinzena_lote
            └─► remuneracao_quinzena_lote → rateio veículo → subsidio_*
```

Cadeia legada `viagem_completa` → `viagem_classificada` → … → `viagem_transacao` permanece para o subsídio R$/km.

**Dims dbt (não YAML OF):** `servico_oferta_faixa`; `lote_servico` ← Anexo I.2
serviço↔lote com vigência `data_inicio`/`data_fim` (fases entrada/plena/
plena_expandida); `operacao_lote` / `operacao_lote_tecnologia` ← Tab.24–26
(`sistema_referencia_lote*`). Sem coluna `rede` no join.

**Tipologia:** classificação (`viagem_valida_classificada`) aplica §5.v via
`tecnologia_servico` (penalidade → `tipo_viagem` → bools). Em
`viagem_classificacao_validacao`, `tecnologia_fcf = coalesce(apurada, mínima)`
(acima da máxima mantém apurada). **Sem `tecnologia_remunerada` no contrato
I.8** (só legado R$/km). FCF tipológico em `fcf_quinzena_lote` usa
`tecnologia_fcf` × `operacao_lote_tecnologia`.

**Faixas horárias:** pressuposto de **cobertura contínua** em
`servico_oferta_faixa` (sem gaps). `viagem_classificacao_validacao` usa
`inner join` na oferta; viagem sem faixa não entra na apuração.

**Flow Prefect:** `treatment__remuneracao_openfisca` → selector
`remuneracao_openfisca`.

**Ainda faltando:** incompleta/`nao_apurada`.

**Depreciado:** `aux_frota_operante_dia_lote` e modelos dbt de IPA/faixa — frota diária e IPA saem do OF. FCF contratual **não** é diário na viagem.

## `viagens_apuradas`

- Grão: `id_apuracao` / `id_viagem`
- Conteúdo: eco + campos §5.6 do [PLANO](PLANO_IMPLEMENTACAO.md) (incl. `ipa`, `frota_operante`, `remuneracao_opex_viagem`, …)

## `fcf_quinzena_lote`

- Grão: `ano × mes × quinzena × lote`
- Frota operante tipológica: distinct veículos completa+pico por
  `tecnologia_fcf` (média em dias úteis)
- Denominador: `operacao_lote_tecnologia` (estimada, teto determinada)
- `fcf_quinzena` = min(1, Σ min(media_tech, estimada_tech) / Σ estimada_tech)
- `km_referencia` = `lote_km_referencia` ou `lote_qr_mensal / 2`

## `remuneracao_quinzena_lote`

```
RQ = TR * (α * km_referencia * fcf_quinzena
         + β * Σ_faixa(qc_km_ponderada_ipa) * (1 - prd))
   - Σ_faixa(desconto_operacao_precaria)
```

- OPEX por viagem já calculado no OF: `remuneracao_opex_viagem = TR * β * km_remuneravel * ipa` (`prd=0`).
- CAPEX + FCF + QR na quinzena (dbt).
- Somar `qc_km_ponderada_ipa` e precária por **faixa distinta** (valores broadcast em cada viagem).
- TR/α/β: eco OF ou dim I.9.

## Rateio / pagamento

- OPEX: usar `remuneracao_opex_viagem` (ou `(km×ipa) / Σ(km×ipa)` se reabrir rateio)
- CAPEX v1: proporcional a `km_remuneravel`
- `remuneracao_veiculo_quinzena` → pagamento
- `subsidio_quinzena_lote` / `subsidio_veiculo_quinzena` (RT + compensação) — **ainda não implementado**

## Modelos dbt (implementados)

| Modelo | Grão | Papel |
|--------|------|-------|
| `viagens_apuradas` | viagem | OF `apurar` |
| `fcf_quinzena_lote` | ano×mes×quinzena×lote | FCF contratual |
| `remuneracao_quinzena_lote` | ano×mes×quinzena×lote | RQ (CAPEX+OPEX−precária) |
| `remuneracao_veiculo_quinzena` | + id_veiculo | Rateio pagamento |

## Nota sobre `arquitetura_funcoes.md`

Parcialmente stale: ainda descreve IPA/RQ dentro do OF com enums antigos e `faixas[]`/`lotes[]`. Preferir este arquivo + PLANO §5.
