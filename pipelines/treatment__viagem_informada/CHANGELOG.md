# CHANGELOG - treatment\_\_viagem_informada

## [1.2.0] - 2026-10-20

### Alterado

- Adiciona a captura `capture__conecta_sistema_rio_viagem_informada` às fontes de dados do flow `treatment__viagem_informada` (https://github.com/RJ-SMTR/pipelines_v3/pull/783)

## [1.1.2] - 2026-10-14

### Removido

- Remove as descrições dos testes das constantes; as notificações usam as descrições definidas no dbt (https://github.com/RJ-SMTR/pipelines_v3/pull/763)

## [1.1.1] - 2026-10-06

### Removido

- Remove `PLANEJAMENTO_DIARIO_SELECTOR` das fontes de dados do flow `treatment__viagem_informada`, eliminando a espera pela atualização do planejamento diário e mantendo as checagens das capturas Rio Ônibus e Maxtrack (https://github.com/RJ-SMTR/pipelines_v3/pull/742).

## [1.1.0] - 2026-08-28

### Adicionado

- Adiciona a fonte Maxtrack ao data_sources do flow `treatment__viagem_informada` (https://github.com/RJ-SMTR/pipelines_v3/pull/577)

## [1.0.0] - 2026-02-26

### Adicionado

- Cria flow `treatment__viagem_informada` (https://github.com/RJ-SMTR/pipelines_v3/pull/68)
