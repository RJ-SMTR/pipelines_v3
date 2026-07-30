# Changelog - capture__stu_tabelas

## [1.2.0] - 2026-06-11

### Adicionado

- Refatora extract_stu_data para detectar registros novos/alterados por hashing por chave primária com PyArrow (https://github.com/RJ-SMTR/pipelines_v3/pull/238)

### Alterado

- Limita o número máximo de workers para 2 execuções simultâneas (https://github.com/RJ-SMTR/pipelines_v3/pull/238)
- Reduz período padrão de recaptura de 5 para 2 dias (https://github.com/RJ-SMTR/pipelines_v3/pull/238)
- Agrupa tabelas menores em um único shedule e ajusta horários (https://github.com/RJ-SMTR/pipelines_v3/pull/238)

### Corrigido

- Corrige chave primária da tabela `mod_chassi` de `cod_mod_chassi_carroceria` para `cod_mod_chassi` (https://github.com/RJ-SMTR/pipelines_v3/pull/238)

## [1.1.0] - 2026-04-27

### Alterado

- Divide a execução agendada em uma tabela por flow run (https://github.com/RJ-SMTR/pipelines_v3/pull/129)

## [1.0.0] - 2026-03-27

### Adicionado

- Cria flow `capture__stu_tabelas` (https://github.com/RJ-SMTR/pipelines_v3/pull/100)
