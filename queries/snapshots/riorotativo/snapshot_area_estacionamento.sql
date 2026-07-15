{% snapshot snapshot_area_estacionamento %}

    {{
        config(
            target_schema="riorotativo_staging",
            unique_key="id_area",
            strategy="timestamp",
            updated_at="timestamp_ultima_atualizacao",
            invalidate_hard_deletes=True,
        )
    }}

    select
        * except (versao),
        timestamp(
            datetime_ultima_atualizacao, "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ ref("area_estacionamento_riorotativo") }}

{% endsnapshot %}
