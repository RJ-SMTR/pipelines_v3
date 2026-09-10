{% snapshot snapshot_ordem_pagamento_entidade_dia %}

    {{
        config(
            target_schema="riorotativo_interno_staging",
            unique_key="id_ordem_pagamento_entidade_dia",
            strategy="timestamp",
            updated_at="timestamp_ultima_atualizacao",
            invalidate_hard_deletes=True,
            partition_by={
                "field": "data_ordem",
                "data_type": "date",
                "granularity": "day",
            },
        )
    }}

    select
        * except (versao, id_execucao_dbt),
        timestamp(
            datetime_ultima_atualizacao, "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ ref("ordem_pagamento_entidade_dia_riorotativo") }}

{% endsnapshot %}
