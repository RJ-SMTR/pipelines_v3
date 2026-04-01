{% snapshot snapshot_log_bigquery %}

    {%- set partition_filter = get_modified_partitions_filter(ref("log_bigquery")) -%}

    {{
        config(
            target_schema="infraestrutura_staging",
            unique_key="id_job",
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
    from {{ ref("log_bigquery") }}
    where {{ partition_filter }}

{% endsnapshot %}
