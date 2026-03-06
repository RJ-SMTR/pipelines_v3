# Changelog - transito_interno

## [1.0.1] - 2026-02-25

### Corrigido

- Corrige filtro de partições no modelo `view_pessoa_fisica_negativacao` para usar `INFORMATION_SCHEMA.PARTITIONS` da `aux_autuacao_negativacao` (https://github.com/RJ-SMTR/pipelines_v3/pull/54)
- Corrige duplicação de registros no modelo `aux_autuacao_negativacao` deduplicando `autuacoes_inclusao` por `id_auto_infracao` e `data_autuacao` (https://github.com/RJ-SMTR/pipelines_v3/pull/54)

### Alterado

- Adiciona `aux_autuacao_negativacao` ao selector `transito_autuacao` no `selectors.yml` (https://github.com/RJ-SMTR/pipelines_v3/pull/54)

## [1.0.0] - 2026-02-02

### Adicionado

- Cria modelos `aux_retorno_negativacao`, `aux_autuacao_negativacao`, `autuacao_negativacao`, `autuacao_controle_negativacao`, `view_pessoa_fisica_negativacao` e `view_autuacao_historico` (https://github.com/RJ-SMTR/pipelines_v3/pull/8)
