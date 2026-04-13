{% snapshot snapshot_viagem_planejada %}

    {{
        config(
            target_schema="projeto_subsidio_sppo_staging",
            unique_key="concat(data, '-', servico, '-', sentido, '-', faixa_horaria_inicio, '-', ifnull(trip_id, ''), '-', ifnull(trip_id_planejado, ''), '-', ifnull(shape_id, ''), '-', ifnull(shape_id_planejado, ''))",
            strategy="timestamp",
            updated_at="timestamp_ultima_atualizacao",
            invalidate_hard_deletes=True,
            partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        )
    }}

    {% set viagem_planejada = ref("viagem_planejada") %}
    {% set target_relation = adapter.get_relation(
        database=this.database, schema=this.schema, identifier=this.name
    ) %}
    {% set table_exists = target_relation is not none %}

    {% if execute and table_exists %}
        {% set modified_partitions_query %}
            select concat("'", parse_date("%Y%m%d", partition_id), "'") as data_particao
            from `{{ viagem_planejada.database }}.{{ viagem_planejada.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ viagem_planejada.identifier }}"
                and partition_id != "__NULL__"
                and datetime(last_modified_time, "America/Sao_Paulo")
                >= datetime_sub(current_datetime("America/Sao_Paulo"), interval 10 day)
        {% endset %}

        {% set modified_partitions = (
            run_query(modified_partitions_query).columns[0].values()
        ) %}

    {% endif %}

    select
        *,
        timestamp(
            datetime_ultima_atualizacao, "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ ref("viagem_planejada") }}
    where
        distancia_total_planejada != 0
        {% if table_exists %}
            and {% if modified_partitions | length > 0 %}
                data in ({{ modified_partitions | join(", ") }})
            {% else %} data = "2000-01-01"
            {% endif %}
        {% endif %}
{% endsnapshot %}
