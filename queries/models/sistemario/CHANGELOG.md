# Changelog - sistemario

## [Unreleased]

### Adicionado

- Dataset `sistemario` / `sistemario_staging` para a cadeia de remuneração
  I.8 (OpenFisca)
- Cadeia a partir de `viagem_valida`: `viagem_valida_classificada`,
  `aux_viagem_valida_temperatura`, `viagem_valida_regularidade_temperatura`,
  `viagem_valida_bilhetagem` → `viagem_classificacao_validacao` →
  `viagens_apuradas` (`openfisca_smtr.apurar`) → `fcf_quinzena_lote` /
  `remuneracao_quinzena_lote` / `remuneracao_veiculo_quinzena`
- Dims `servico_oferta_faixa`, `lote_servico`, `operacao_lote`,
  `operacao_lote_tecnologia` (seeds `sistema_referencia_*`)
- Flow Prefect `treatment__remuneracao_openfisca` (selector
  `remuneracao_openfisca`)
- Simulação **lote A0** (jul/2026): 452 serviços + stub
  `operacao_lote`/`_tecnologia` (cópia A2 plena); OpenFisca `Lote.A0`
- `tecnologia_fcf` em `viagem_classificacao_validacao` / `viagens_apuradas`
- FCF tipológico (I.8 §4) em `fcf_quinzena_lote` vs `operacao_lote_tecnologia`

### Pendente / anotado (não implementar agora)

- Incompleta/`nao_apurada`; cobertura `valor_km.indicador_validade|conformidade`
- Frota real A0 (stub atual = cópia A2 plena)

### Alterado

- Modelos da cadeia I.8 movidos de `subsidio` para `sistemario`
- `viagem_classificacao_validacao`: `inner join` em faixa;
  `servico_tecnologia` via `tecnologia_servico`; sem `tecnologia_remunerada`
  no contrato I.8
- Seeds `sistema_referencia_*` no schema `sistemario` e no selector
  `remuneracao_openfisca` (carregar com `dbt seed` / `dbt build`)
- Substitui chave `rede` por vigência `data_inicio`/`data_fim` nas seeds
  I.2 e dims (`lote_servico`, `operacao_lote*`); join na viagem só por data
  (OI 2026-08-01 → plena em +9m; plena_expandida B2 em 2028-08-25)
- `viagem_valida_classificada`: base temporária `viagem_completa`
  (`servico_realizado`) para testes — `viagem_valida` ainda sem dados
- Params OpenFisca com vigência a partir de `2026-07-01` (simulação A0)
- Contrato `apurar(viagens, planejamento)`: lote/programadas em
  `servico_oferta_faixa`; classificação só fatos + faixa; FCF lê
  `operacao_lote`; RQ soma R$ 1.200 por faixa POR vazia nos dias
  apurados; `prd=0` (stub IDT=1)
- WIP/teste: `lote_padrao` = A0 na ausência de lote em
  `servico_oferta_faixa` (macro `lote_padrao_teste`). Remover var +
  macro + chamada após o teste.
- `viagens_apuradas`: chave da simulação = `id_viagem` (`id_key`);
  sem `id_apuracao`; o modelo só lê as duas tabelas, chama `apurar`
  e persiste o schema BQ (coerção de tipos no pacote OF)
