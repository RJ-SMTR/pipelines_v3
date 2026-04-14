{% macro get_modified_partitions_filter(
    source_relation, partition_column="data", interval_days=10
) %}
    {%- set target_relation = adapter.get_relation(
        database=this.database, schema=this.schema, identifier=this.name
    ) -%}
    {%- set table_exists = target_relation is not none -%}

    {%- if execute and table_exists -%}
        {%- set query -%}
            select concat("'", parse_date("%Y%m%d", partition_id), "'") as data_particao
            from `{{ source_relation.database }}.{{ source_relation.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ source_relation.identifier }}"
                and partition_id != "__NULL__"
                and datetime(last_modified_time, "America/Sao_Paulo")
                >= datetime_sub(current_datetime("America/Sao_Paulo"), interval {{ interval_days }} day)
        {%- endset -%}

        {%- set partitions = run_query(query).columns[0].values() -%}

        {%- if partitions | length > 0 -%}
            {{ partition_column }} in ({{ partitions | join(", ") }})
        {%- else -%} {{ partition_column }} = "2022-01-01"
        {%- endif -%}
    {%- else -%} true
    {%- endif -%}
{% endmacro %}
