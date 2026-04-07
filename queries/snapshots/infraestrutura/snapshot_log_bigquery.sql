{% snapshot snapshot_log_bigquery %}

    {{
        config(
            target_schema="infraestrutura_staging",
            unique_key="id_job",
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
    from {{ ref("log_bigquery") }}

{% endsnapshot %}
