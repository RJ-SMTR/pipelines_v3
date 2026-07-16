{% snapshot snapshot_perfil_funcionamento %}

    {{
        config(
            target_schema="riorotativo_staging",
            unique_key="id_perfil_funcionamento",
            strategy="timestamp",
            updated_at="timestamp_ultima_atualizacao",
            invalidate_hard_deletes=True,
            enabled=is_current_state_enabled()
        )
    }}

    select
        * except (versao),
        timestamp(
            datetime_ultima_atualizacao, "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ ref("perfil_funcionamento_riorotativo") }}

{% endsnapshot %}
