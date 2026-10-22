{% if var("start_date") >= var("DATA_SUBSIDIO_V14_INICIO") %}
    {{ config(enabled=false) }}
{% else %}
    {{
        config(
            materialized="incremental",
            partition_by={"field": "data", "data_type": "date", "granularity": "day"},
            incremental_strategy="insert_overwrite",
        )
    }}
{% endif %}

with
    subsidio_dia as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            safe_cast(avg(pof) as numeric) as media_pof,
            safe_cast(coalesce(stddev(pof), 0) as numeric) as desvp_pof
        from {{ ref("percentual_operacao_faixa_horaria") }}
        -- from `rj-smtr.subsidio.percentual_operacao_faixa_horaria`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
        group by data, tipo_dia, consorcio, servico
    ),
    valores_subsidio as (
        select *
        from {{ ref("subsidio_sumario_servico_dia_pagamento") }}
        -- from `rj-smtr.financeiro.subsidio_sumario_servico_dia_pagamento`
        where
            data
            between date('{{ var("start_date") }}') and date('{{ var("end_date") }}')
            and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
    ),
    pivot_data as (
        select *
        from
            (
                select data, tipo_dia, consorcio, servico, tipo_viagem, km_apurada_faixa
                from {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }}
                -- from `rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem`
                where
                    data between date('{{ var("start_date") }}') and date(
                        '{{ var("end_date") }}'
                    )
                    and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
            ) pivot (
                sum(km_apurada_faixa) as km_apurada for tipo_viagem in (
                    "Registrado com ar inoperante" as registrado_com_ar_inoperante,
                    "Não licenciado" as n_licenciado,
                    "Autuado por ar inoperante" as autuado_ar_inoperante,
                    "Autuado por segurança" as autuado_seguranca,
                    "Autuado por limpeza/equipamento" as autuado_limpezaequipamento,
                    "Licenciado sem ar e não autuado" as licenciado_sem_ar_n_autuado,
                    "Licenciado com ar e não autuado" as licenciado_com_ar_n_autuado,
                    "Não vistoriado" as n_vistoriado,
                    "Sem transação" as sem_transacao
                )
            )
    )
select
    vs.data,
    vs.tipo_dia,
    vs.consorcio,
    vs.servico,
    vs.viagens_dia,
    vs.km_apurada_dia,
    vs.km_subsidiada_dia,
    vs.km_planejada_dia,
    sd.media_pof,
    sd.desvp_pof,
    coalesce(
        km_apurada_registrado_com_ar_inoperante, 0
    ) as km_apurada_registrado_com_ar_inoperante,
    coalesce(km_apurada_n_licenciado, 0) as km_apurada_n_licenciado,
    coalesce(km_apurada_autuado_ar_inoperante, 0) as km_apurada_autuado_ar_inoperante,
    coalesce(km_apurada_autuado_seguranca, 0) as km_apurada_autuado_seguranca,
    coalesce(
        km_apurada_autuado_limpezaequipamento, 0
    ) as km_apurada_autuado_limpezaequipamento,
    coalesce(
        km_apurada_licenciado_sem_ar_n_autuado, 0
    ) as km_apurada_licenciado_sem_ar_n_autuado,
    coalesce(
        km_apurada_licenciado_com_ar_n_autuado, 0
    ) as km_apurada_licenciado_com_ar_n_autuado,
    coalesce(km_apurada_n_vistoriado, 0) as km_apurada_n_vistoriado,
    coalesce(km_apurada_sem_transacao, 0) as km_apurada_sem_transacao,
    vs.valor_a_pagar,
    vs.valor_glosado,
    vs.valor_acima_limite,
    vs.valor_total_sem_glosa,
    vs.valor_total_apurado,
    vs.valor_judicial,
    vs.valor_penalidade,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from valores_subsidio as vs
left join subsidio_dia as sd using (data, tipo_dia, consorcio, servico)
left join pivot_data as pd using (data, tipo_dia, consorcio, servico)
