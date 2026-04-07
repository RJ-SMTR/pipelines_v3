# Changelog

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.

O formato está baseado em [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
e este projeto tenta aderir ao [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-03-19

### Adicionado

- Migração de Prefect 1.4 para Prefect 3.0
- Implementação de captura de licenciamento de veículos SPPO via FTP RDO
- Fallback para GCS em caso de indisponibilidade do FTP
- Configuração de deployment para staging e produção
- Schedule diário às 7h da manhã (horário de São Paulo)
- Suporte a recaptura de dados com janela configurável (padrão: 2 dias)
