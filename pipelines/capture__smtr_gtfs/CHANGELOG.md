# Changelog - capture__smtr_gtfs

## [1.0.0] - 2026-10-05

### Adicionado

- Cria o flow de captura do GTFS sobre o flow genérico de captura, com gate de OS via Redis (`last_captured_os`) e disparo do `treatment__gtfs` ao fim da captura (https://github.com/RJ-SMTR/pipelines_v3/pull/736)
