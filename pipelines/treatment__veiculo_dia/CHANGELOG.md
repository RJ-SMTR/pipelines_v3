# Changelog

Todas as mudanças notáveis neste projeto serão documentadas neste arquivo.

## [0.1.0] - 2026-05-07

### Added

- Migração inicial da pipeline `treatment__veiculo_dia` de Prefect 1.4 para Prefect 3.0
- Implementação do flow de materialização usando `create_materialization_flows_default_tasks()`
- Configuração de selectors DBT para `veiculo_dia` e `snapshot_veiculo_dia`
- Testes DBT configurados com validações de dados
- Deployment em staging e produção com agendamento diário às 6:15
