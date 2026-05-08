{% test sincronizacao_tabelas(
    model,
    tabela_base,
    filtro_tabela_base,
    join_key,
    excluir_colunas=None,
    filtro_particao_modificada=False
) %}

    {% set pattern = "`([^`]+)`\.`([^`]+)`\.`([^`]+)`" %}
    {% set match = modules.re.search(pattern, model) %}
    {% set project = match[1] %}
    {% set dataset = match[2] %}
    {% set table = match[3] %}

    {% if execute and filtro_particao_modificada %}
        {% set data_partitions_query %}
            select concat("'", parse_date("%Y%m%d", partition_id), "'") as data
            from `{{ project }}.{{ dataset }}.INFORMATION_SCHEMA.PARTITIONS`
            where
            table_name = "{{ table }}"
            and partition_id != "__NULL__"
            and datetime(last_modified_time, "America/Sao_Paulo") between
                datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
        {% endset %}

        {% set partitions = run_query(data_partitions_query).columns[0].values() %}
    {% endif %}

    {% set relation = adapter.get_relation(
        database=project, schema=dataset, identifier=table
    ) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% set column_names = (
        columns
        | map(attribute="name")
        | reject(
            "equalto",
            join_key,
        )
        | list
    ) %}

    {% if excluir_colunas %}
        {% set column_names = (
            column_names
            | reject(
                "in",
                excluir_colunas,
            )
            | list
        ) %}
    {% endif %}

    {% set select_columns = column_names | join(", ") %}

    with
        a as (
            select {{ join_key }}, {{ select_columns }}
            from {{ model }}
            {% if filtro_particao_modificada %}
                where
                    {% if partitions | length > 0 %}
                        data in ({{ partitions | join(", ") }})
                    {% else %} false
                    {% endif %}
            {% endif %}
        ),
        b as (
            select {{ join_key }}, {{ select_columns }}
            from {{ tabela_base }}
            where
                ({{ filtro_tabela_base }})
                {% if filtro_particao_modificada %}
                    and {% if partitions | length > 0 %}
                        data in ({{ partitions | join(", ") }})
                    {% else %} false
                    {% endif %}
                {% endif %}
        ),
        compare as (
            select
                coalesce(a.{{ join_key }}, b.{{ join_key }}) as {{ join_key }},
                {% for col in column_names %}
                    coalesce(cast(a.{{ col }} as string), '')
                    != coalesce(cast(b.{{ col }} as string), '') as diff_{{ col }}
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            from a
            full outer join b using ({{ join_key }})
        )
    select *
    from compare
    where
        {% for col in column_names %}
            diff_{{ col }} {% if not loop.last %} or {% endif %}
        {% endfor %}

{% endtest %}
