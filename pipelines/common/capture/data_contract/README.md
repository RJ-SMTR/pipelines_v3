# Contratos ODCS gerados a partir do dbt

## Ambiente e geração

Dependência opcional, validada com `datacontract-cli[duckdb]==1.2.1`.
Para manter esse ambiente separado do ambiente Prefect/dbt, execute na raiz do projeto:

```bash
uv venv /tmp/datacontract-venv
uv --no-config pip install --python /tmp/datacontract-venv/bin/python \
  -r pipelines/common/capture/data_contract/requirements.txt
export DATACONTRACT_PYTHON=/tmp/datacontract-venv/bin/python
```

O caminho em `/tmp` serve para desenvolvimento. Em uma imagem, instale as mesmas
dependências em um caminho persistente e configure `DATACONTRACT_PYTHON`.
Sem essa variável, a task usa o Python do próprio processo.
Importer e execução dos testes sempre usam o mesmo interpretador.

Atualize o manifest com dbt Core, a partir de `queries/`:

```bash
../.venv/bin/dbt parse --profiles-dir ./dev
```

Depois, na raiz do projeto, gere o contrato:

```bash
"$DATACONTRACT_PYTHON" pipelines/common/capture/data_contract/dbt_importer.py \
  --manifest queries/target/manifest.json \
  --model base_feed_info \
  --output /tmp/base_feed_info.odcs.yaml
```

O importer não executa SQL/Jinja, não materializa modelos e não consulta o BigQuery.
Colunas e testes devem descrever os dados que existem no arquivo bruto; testes de
modelos transformados não são automaticamente aplicáveis à captura.

A task regenera o ODCS em cada execução, adapta as colunas técnicas e as chaves do
`SourceTable`, persiste o artefato e testa uma cópia temporária contra cada arquivo.
O manifest precisa estar atualizado. O servidor `incoming` só existe nessa cópia.
Uma falha de importação ou validação é propagada ao chamador.
`context.source.raw_filetype` deve representar o formato aceito pelo CLI, como `csv`,
mesmo que o arquivo tenha extensão `.txt`. A contagem de linhas é por arquivo.

## Testes traduzidos

| Teste dbt | Regra ODCS | Restrição |
| --- | --- | --- |
| `not_null` | Coluna: `nullValues`, `mustBe: 0` | Sem filtro ou limiar customizado. |
| `unique` | Coluna: `duplicateValues`, `mustBe: 0` | Ignora nulos, como o teste dbt; não cria chave primária. |
| `accepted_values` | Coluna: `invalidValues`, `arguments.validValues` | `quote: true` ou padrão; valores são convertidos em literais de texto, como na macro. |
| `dbt_utils.unique_combination_of_columns` | Modelo: `duplicateValues`, `arguments.properties` | Todas as colunas precisam de `not_null` com severidade `error` ou constraint explícita. |
| `dbt_expectations.expect_table_row_count_to_equal` | Modelo: `rowCount`, `mustBe` | `value` numérico finito. |
| `dbt_expectations.expect_table_row_count_to_be_between` | Modelo: `rowCount`, `mustBeBetween` | Os dois limites são obrigatórios, inclusivos e finitos. |

Exemplo de autoria no YAML dbt:

```yaml
models:
  - name: base_feed_info
    data_tests:
      - dbt_expectations.expect_table_row_count_to_equal:
          arguments:
            value: 1
```

Testes desabilitados são ignorados. `severity: warn` vira `severity: warning`.
Outros testes habilitados, SQL singular, filtros (`where`, `row_condition`, `group_by`),
`limit`, limites customizados de falha e argumentos não suportados provocam erro.
Opções adicionais como `strictly` e `quote_columns` ainda não são traduzidas.
Não há execução das macros dbt: as traduções pressupõem as macros padrão dos pacotes,
sem sobrescritas locais que mudem sua semântica.

A restrição de não nulidade na unicidade composta evita uma perda de cobertura:
o dbt_utils agrupa chaves nulas, enquanto a biblioteca do datacontract as exclui.
Ela preserva a aprovação/reprovação do conjunto de regras; as contagens individuais
podem diferir quando o arquivo viola a regra de não nulidade.

## Regras de biblioteca declaradas em meta

Modelos e colunas podem declarar `config.meta.datacontract_cli.quality`.
O formato legado `meta.datacontract_cli.quality` também é aceito, com preferência
por `config.meta` quando presente. Exemplo de coluna:

```yaml
columns:
  - name: feed_lang
    config:
      meta:
        datacontract_cli:
          quality:
            - type: library
              metric: missingValues
              arguments:
                missingValues: [null, '', 'N/A']
              mustBe: 0
```

São aceitas as métricas `rowCount`, `duplicateValues`, `nullValues`, `missingValues`
e `invalidValues`, nos escopos suportados pelo CLI. Métricas, argumentos e comparadores
inválidos são rejeitados. Regras SQL/custom não entram neste importer.
Uma regra declarada somente em `meta` não vira um teste executado pelo dbt.

## API Python e testes

Importar o módulo registra o formato `dbt-quality`, sem substituir o formato `dbt`:

```python
from datacontract.data_contract import DataContract
from pipelines.common.capture.data_contract.dbt_importer import import_contract_from_manifest

contract_dict = import_contract_from_manifest("queries/target/manifest.json", "base_feed_info")
contract = DataContract.import_from_source(
    format="dbt-quality",
    source="queries/target/manifest.json",
    dbt_model=["base_feed_info"],
)
```

Esse formato personalizado é acessível pela API Python e pelo wrapper acima;
não é adicionado automaticamente ao comando standalone `datacontract import`.

```bash
"$DATACONTRACT_PYTHON" -m unittest pipelines.common.capture.data_contract.tests.test_dbt_importer -v
.venv/bin/python -m unittest pipelines.common.capture.data_contract.tests.test_tasks \
  pipelines.common.capture.data_contract.tests.test_utils -v
.venv/bin/ruff check pipelines/common/capture/data_contract
```
