# Changelog - capture__serpro_autuacao

## [1.1.1] - 2026-07-20

### Alterado

- Define `timeout_seconds=18000` (5h) no flow

## [1.1.0] - 2026-06-11

### Alterado

- Substitui o download do certificado SSL por leitura do secret `radar_serpro_v2_crt` e gravação local do PEM (https://github.com/RJ-SMTR/pipelines_v3/pull/241)
- Adiciona diagnóstico TCP/SSL e recuperação da cadeia de certificados do SERPRO com artefato Prefect para atualização do secret (https://github.com/RJ-SMTR/pipelines_v3/pull/241)

## [1.0.2] - 2026-05-12

### Alterado

- Define `timeout_seconds=10800` (3h) no flow (https://github.com/RJ-SMTR/pipelines_v3/pull/176)

## [1.0.1] - 2026-01-09

### Alterado

- Ativa recaptura no flow (https://github.com/RJ-SMTR/pipelines_v3/pull/21)

## [1.0.0] - 2025-12-17

### Adicionado

- Cria flow de captura para serpro_autuacao (https://github.com/RJ-SMTR/pipelines_v3/pull/7)
