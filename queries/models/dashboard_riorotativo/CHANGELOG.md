# Changelog - dashboard_riorotativo

## [1.0.1] - 2026-09-25

### Alterado

- Ajusta cálculo de `quantidade_vaga_fisica` nos modelos `view_ativacao_dia_riorotativo` e `view_ativacao_hora_riorotativo` para usar `ceiling` ao dividir vagas de moto por 5, garantindo arredondamento para cima

## [1.0.0] - 2026-09-01

### Adicionado

- Cria modelos `view_ativacao_dia_riorotativo.sql` e `view_ativacao_hora_riorotativo.sql` (https://github.com/RJ-SMTR/pipelines_v3/pull/561)