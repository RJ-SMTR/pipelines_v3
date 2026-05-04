{% snapshot snapshot_sppo_veiculo_dia %}

    {{
        config(
            target_schema="subsidio_staging",
            unique_key="concat(data, '-', id_veiculo)",
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
    from {{ ref("sppo_veiculo_dia") }}
    where data < date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")

{% endsnapshot %}
