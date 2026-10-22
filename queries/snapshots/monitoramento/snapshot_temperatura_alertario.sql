{% snapshot snapshot_temperatura_alertario %}

    {{
        config(
            target_schema="monitoramento_staging",
            unique_key="concat(data, '-', hora, '-', id_estacao)",
            strategy="timestamp",
            updated_at="timestamp_ultima_atualizacao",
            invalidate_hard_deletes=True,
            partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        )
    }}

    select
        *,
        timestamp(
            datetime_ultima_atualizacao, "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ ref("temperatura_alertario") }}

{% endsnapshot %}
