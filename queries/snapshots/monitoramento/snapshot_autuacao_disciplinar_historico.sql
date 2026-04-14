{% snapshot snapshot_autuacao_disciplinar_historico %}

    {{
        config(
            target_schema="monitoramento_staging",
            unique_key="id_auto_infracao",
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
    from {{ ref("autuacao_disciplinar_historico") }}

{% endsnapshot %}
