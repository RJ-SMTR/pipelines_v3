{% snapshot snapshot_viagem_planejada_planejamento %}

    {%- set partition_filter = get_modified_partitions_filter(ref("viagem_planejada_planejamento")) -%}

    {{
        config(
            target_schema="planejamento_staging",
            unique_key="id_viagem",
            strategy="timestamp",
            updated_at="timestamp_ultima_atualizacao",
            invalidate_hard_deletes=True,
            partition_by={"field": "data", "data_type": "date", "granularity": "day"},
            partition_filter=partition_filter,
        )
    }}

    select
        *,
        timestamp(
            datetime_ultima_atualizacao, "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ ref("viagem_planejada_planejamento") }}
    where {{ partition_filter }}

{% endsnapshot %}
