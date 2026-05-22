{% snapshot snapshot_subsidio_sumario_servico_dia_pagamento %}
    {% if var("start_date") >= var("DATA_SUBSIDIO_V14_INICIO") %}
        {{
            config(
                enabled=false,
                target_schema="financeiro_staging",
                unique_key="concat(data, '-', servico)",
                strategy="timestamp",
                updated_at="timestamp_ultima_atualizacao",
            )
        }}
    {% else %}
        {{
            config(
                target_schema="financeiro_staging",
                unique_key="concat(data, '-', servico)",
                strategy="timestamp",
                updated_at="timestamp_ultima_atualizacao",
                invalidate_hard_deletes=True,
                partition_by={
                    "field": "data",
                    "data_type": "date",
                    "granularity": "day",
                },
            )
        }}
    {% endif %}

    select
        * except (versao),
        timestamp(
            datetime_ultima_atualizacao, "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ ref("subsidio_sumario_servico_dia_pagamento") }}

{% endsnapshot %}
