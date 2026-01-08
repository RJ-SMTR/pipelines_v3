# Changelog - cadastro

## [1.7.5] - 2025-12-19

### Alterado

- Acrescenta exceção de ajuste no modelo `veiculo_licenciamento_dia` para correção de tecnologia, conforme MTR-CAP-2025/59482 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1129)

## [1.7.4] - 2025-12-15

### Alterado

- Acrescenta exceção de ajuste no modelo `veiculo_licenciamento_dia` para correção de tecnologia, conforme MTR-CAP-2025/59482 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1114)

## [1.7.3] - 2025-12-09

### Alterado

- Altera exceção de ajuste no modelo `veiculo_licenciamento_dia` para correção de tecnologia para Novembro Q2, conforme MTR-CAP-2025/59482 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1095)

## [1.7.2] - 2025-12-04

### Corrigido

- Corrige tratamento da `data_ultima_vistoria` para veículos que trocaram de placa no modelo `veiculo_licenciamento_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1082)

## [1.7.1] - 2025-12-02

### Alterado

- Acrescenta exceção de ajuste no modelo `veiculo_licenciamento_dia` para correção de tecnologia, conforme MTR-CAP-2025/59482 (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1078)

## [1.7.0] - 2025-11-13

### Adicionado

- Adiciona filtro nos testes do modelo `veiculo_licenciamento_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1056)
- Adiciona teste de nulidade nas colunas `data_ultima_vistoria` e `ano_ultima_vistoria` do modelo `veiculo_licenciamento_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1056)

## [1.6.9] - 2025-11-11

### Adicionado

- Adiciona tratamento da `data_ultima_vistoria` para veículos que trocaram de placa no modelo `veiculo_licenciamento_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1048)
- Adiciona coluna `indicador_data_ultima_vistoria_tratada` no modelo `veiculo_licenciamento_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1048)

## [1.6.8] - 2025-11-04

### Removido

- Remoção das colunas `operador`, `indicador_ativa`, `tipo_uso` e `area` no modelo `garagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/1025)

## [1.6.7] - 2025-10-27

### Adicionado

- Adiciona modelos `staging_garagem` e `garagem` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/996)

## [1.6.6] - 2025-10-14

### Corrigido

- Corrigir tratamento de data inicial retroativa no modelo `veiculo_licenciamento_dia` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/939)

## [1.6.5] - 2025-09-29

### Alterado

- Altera fonte dos dados de servico da Jaé dos modelos `servicos.sql` e `servico_operadora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/892)

## [1.6.4] - 2025-09-23

### Adicionado

- Adiciona modelos `modos.sql` e `aux_consorcio_modo.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/886)

## [1.6.3] - 2025-08-27

### Alterado

- Adiciona conversão de datetime nas colunas `timestamp_captura` e `dt_cadastro` no modelo `staging_cliente.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/812)

## [1.6.2] - 2025-08-04

### Adicionado

Cria modelo `aux_servico_jae.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/755)

### Alterado

- Faz deduplicação da tabela `staging_linha` nos modelos `servico_operadora.sql` e `servicos.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/755)

## [1.6.1] - 2025-08-04

### Adicionado

- Cria modelo `aux_linha_tarifa.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/742)

### Alterado
- Substitui CTE linha_tarifa pelo modelo `aux_linha_tarifa.sql` no modelo `servico_operadora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/742)

## [1.6.0] - 2025-07-03

### Adicionado

- Move modelos do dataset `br_rj_riodejaneiro_bilhetagem_staging` para `cadastro_staging` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/505):
  - `staging_cliente.sql`
  - `staging_consorcio.sql`
  - `staging_conta_bancaria.sql`
  - `staging_contato_pessoa_juridica.sql`
  - `staging_endereco.sql`
  - `staging_linha_consorcio_operadora_transporte.sql`
  - `staging_linha_consorcio.sql`
  - `staging_linha_sem_ressarcimento.sql`
  - `staging_linha_tarifa.sql`
  - `staging_linha.sql`
  - `staging_operadora_transporte.sql`

## [1.5.0] - 2025-06-25

### Adicionado
- Cria modelo `veiculo_licenciamento_dia.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)
- Adiciona coluna `ano_ultima_vistoria` no modelo `staging_licenciamento_stu.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)

### Alterado
- Move modelo `licenciamento_stu_staging.sql` do dataset `veiculo_staging` para `cadastro_staging` e renomeia para `staging_licenciamento_stu.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/632)

## [1.4.0] - 2025-02-17

### Alterado
- Adiciona informação de tarifa no modelo `servico_operadora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/435)

## [1.3.1] - 2024-10-08

### Alterado
- Adiciona TEC no modelo `operadoras.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/265)

## [1.3.0] - 2024-09-10

### Adicionado
- Cria modelo `servico_operadora.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/191)

## [1.2.1] - 2024-08-02

### Alterado
- Adiciona tag `geolocalizacao` ao modelo `servicos.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/127)
- Adiciona tag `identificacao` ao modelo `operadoras.sql` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/127)

## [1.2.0] - 2024-07-17

### Adicionado

- Cria modelos auxiliares para a tabela de servicos:
  - `aux_routes_vigencia_gtfs.sql`
  - `aux_stops_vigencia_gtfs.sql`
  - `aux_servicos_gtfs.sql`

### Alterado

- Altera estrutura do modelo `servicos.sql` para adicionar datas de inicio e fim de vigência (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/98)
- Altera filtro no modelo `operadoras.sql`, deixando de filtrar operadores do modo `Fretamento` (https://github.com/prefeitura-rio/pipelines_rj_smtr/pull/98)

## [1.1.1] - 2024-04-25

### Alterado

- Adicionada coluna de modo na tabela `cadastro.consorcio`(https://github.com/prefeitura-rio/queries-rj-smtr/pull/282)
- Adicionadas descrições das colunas modo e razão social no schema (https://github.com/prefeitura-rio/queries-rj-smtr/pull/282)
- Adicionado source para `cadastro_staging.consorcio_modo` (https://github.com/prefeitura-rio/queries-rj-smtr/pull/282)


## [1.1.0] - 2024-04-18

### Alterado

- Filtra dados dos modos Escolar, Táxi, TEC e Fretamento no modelo `operadoras.sql`
- Altera join da Jaé com o STU no modelo `operadoras.sql`, considerando o modo BRT como ônibus, para ser possível ligar a MobiRio (https://github.com/prefeitura-rio/queries-rj-smtr/pull/273)

### Corrigido

- Reverte o tratamento do modelo `consorcios.sql` visto que a MobiRio está cadastrada na nova extração dos operadores no STU (https://github.com/prefeitura-rio/queries-rj-smtr/pull/273)

## [1.0.1] - 2024-04-16

### Corrigido

- Mudança no tratamento do modelo `consorcios.sql` para que o consórcio antigo do BRT não fique relacionado à MobiRio (https://github.com/prefeitura-rio/queries-rj-smtr/pull/272)