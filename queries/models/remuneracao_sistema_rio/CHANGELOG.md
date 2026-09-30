# Changelog - remuneracao_sistema_rio

## [1.0.0] - 2026-09-30

### Adicionado

- Cria a cadeia de remuneração I.8 no dataset `remuneracao_sistema_rio`:
  classificação da `viagem_valida`, apuração OpenFisca e sumários por
  faixa, dia e quinzena (https://github.com/RJ-SMTR/pipelines_v3/pull/576)
- Adiciona `lote_km_rede_referencia` com a km mensal da rede por lote e
  vigência. A rede de entrada vale de 16/08/2026 a 15/06/2027 e a rede
  plena a partir de 16/06/2027. `servico_oferta_faixa` usa a vigência da
  data e a quinzena fica com a metade
  (https://github.com/RJ-SMTR/pipelines_v3/pull/576)
- Calcula o OPEX da faixa como tarifa × β × km das viagens válidas × IPA.
  O IPA usa o percentual de atendimento em escala 0–100
  (https://github.com/RJ-SMTR/pipelines_v3/pull/576)
- Calcula o CAPEX da quinzena como tarifa × α × (km da quinzena / 15) ×
  dias da quinzena × FCF. Na segunda quinzena de setembro de 2026 os dias
  vão de 23/09 a 30/09 (https://github.com/RJ-SMTR/pipelines_v3/pull/576)
- Soma a receita da quinzena com `valor_total_transacao_bruto` de
  `bilhetagem_consorcio_operador_dia` por dia e consórcio, com dia igual
  a `data_ordem` menos 1 (https://github.com/RJ-SMTR/pipelines_v3/pull/576)
- Aplica ISS e IRRF sobre o bruto limitado a zero
  (https://github.com/RJ-SMTR/pipelines_v3/pull/576)
