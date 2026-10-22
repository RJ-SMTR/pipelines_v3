{% snapshot snapshot_percentual_operacao_faixa_horaria %}

    {{
        config(
            target_schema="subsidio_staging",
            unique_key="concat(data, '-', faixa_horaria_inicio, '-', servico, ifnull(concat('-', cast(sentido as string)), ''))",
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
    from {{ ref("percentual_operacao_faixa_horaria") }}

{% endsnapshot %}
