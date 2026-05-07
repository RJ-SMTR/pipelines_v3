# Changelog - datario

## [1.0.3] - 2026-05-06

### Alterado

- Altera modelo `gps_brt_datario` para referenciar `view_gps_brt_completo` e recalcular as colunas `flag_em_movimento`, `tipo_parada` e `flag_linha_existe_sigmob` a partir das fontes legada e nova (https://github.com/RJ-SMTR/pipelines_v3/pull/148)
- Refatora o modelo `gps_onibus_datario` (https://github.com/RJ-SMTR/pipelines_v3/pull/162)

## [1.0.2] - 2025-11-05

### Alterado
- Atualiza os modelos `licenciamento_veiculo_datario.sql` e `gps_onibus_datario.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1030)

## [1.0.1] - 2025-02-12

### Adicionado
- Cria modelo para view do datario `licenciamento_veiculo_datario.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/426)

## [1.0.0] - 2024-12-16

### Adicionado
- Cria modelos para views do datario (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/361):
    - `gps_brt_datario.sql`
    - `gps_onibus_datario.sql`
    - `viagem_onibus_datario.sql`