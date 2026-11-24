{% macro source_csv_parquet(source_name, table_name, partition_column="data") %}
    {#
    Lê o source em CSV até a véspera de DATA_SOURCE_PARQUET_INICIO e em Parquet a partir
    dessa data. Enquanto a tabela Parquet não existir, lê somente o CSV.
    #}
    {% set csv_relation = source(source_name, table_name) %}
    {% set parquet_relation = source(source_name ~ "_parquet", table_name) %}
    {% if execute and table_exists(parquet_relation) %}
        {% set data_inicio = var("DATA_SOURCE_PARQUET_INICIO") %}
        {% set columns = (
            adapter.get_columns_in_relation(csv_relation)
            | map(attribute="name")
            | join(", ")
        ) %}
        (
            select {{ columns }}
            from {{ csv_relation }}
            where {{ partition_column }} < date("{{ data_inicio }}")
            union all
            select {{ columns }}
            from {{ parquet_relation }}
            where {{ partition_column }} >= date("{{ data_inicio }}")
        )
    {% else %} {{ csv_relation }}
    {% endif %}
{% endmacro %}
