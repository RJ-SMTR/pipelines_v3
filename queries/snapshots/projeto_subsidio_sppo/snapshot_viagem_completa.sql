{% snapshot snapshot_viagem_completa %}

    {{
        config(
            target_schema="projeto_subsidio_sppo_staging",
            unique_key="id_viagem",
            strategy="timestamp",
            updated_at="timestamp_ultima_atualizacao",
            invalidate_hard_deletes=True,
            partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        )
    }}

    {% set viagem_completa = ref("viagem_completa") %}
    {% set target_relation = adapter.get_relation(
        database=this.database, schema=this.schema, identifier=this.name
    ) %}
    {% set table_exists = target_relation is not none %}

    {% if execute and table_exists %}
        {% set modified_partitions_query %}
            select concat("'", parse_date("%Y%m%d", partition_id), "'") as data_particao
            from `{{ viagem_completa.database }}.{{ viagem_completa.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ viagem_completa.identifier }}"
                and partition_id != "__NULL__"
                and datetime(last_modified_time, "America/Sao_Paulo")
                >= datetime_sub(current_datetime("America/Sao_Paulo"), interval 10 day)
        {% endset %}

        {% set modified_partitions = (
            run_query(modified_partitions_query).columns[0].values()
        ) %}

    {% endif %}

    select
        * except (versao_modelo),
        timestamp(
            datetime_ultima_atualizacao, "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ viagem_completa }}
    {% if table_exists %}
        where
            {% if modified_partitions | length > 0 %}
                data in ({{ modified_partitions | join(", ") }})
            {% else %} data = "2000-01-01"
            {% endif %}
    {% endif %}
    qualify
        row_number() over (
            partition by id_viagem order by datetime_ultima_atualizacao desc
        )
        = 1

{% endsnapshot %}
