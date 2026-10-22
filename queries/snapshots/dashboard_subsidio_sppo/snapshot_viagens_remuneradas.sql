{% snapshot snapshot_viagens_remuneradas %}

    {{
        config(
            target_schema="dashboard_subsidio_sppo_staging",
            unique_key="id_viagem",
            strategy="timestamp",
            updated_at="timestamp_ultima_atualizacao",
            invalidate_hard_deletes=True,
            partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        )
    }}

    select
        * except (versao),
        timestamp(
            datetime_ultima_atualizacao, "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ ref("viagens_remuneradas") }}

{% endsnapshot %}
