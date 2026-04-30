{% set is_disabled = var("start_date") < var("DATA_SUBSIDIO_V14_INICIO") %}

{{ config(materialized="ephemeral") }}

with
    sumario_faixa_servico_dia as (
        select
            sdp.data,
            sdp.tipo_dia,
            sdp.consorcio,
            sdp.servico,
            sum(sdp.viagens_faixa) as viagens_dia,
            sum(sdp.km_apurada_faixa) as km_apurada,
            sum(sdp.km_planejada_faixa) as km_planejada_dia,
            sum(sdp.valor_a_pagar) as valor_a_pagar,
            sum(sdp.valor_penalidade) as valor_penalidade
        from {{ ref("sumario_faixa_servico_dia_pagamento") }} as sdp
        -- `rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia_pagamento`
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
            and data >= date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
        group by data, tipo_dia, consorcio, servico
    ),
    {% if is_disabled %}
        sumario_servico_dia as (
            select
                data,
                sdp.tipo_dia,
                sdp.consorcio,
                servico,
                sdp.viagens_dia,
                km_apurada_dia as km_apurada,
                km_planejada_dia,
                valor_a_pagar,
                valor_penalidade
            from {{ ref("subsidio_sumario_servico_dia_pagamento") }} as sdp
            where
                data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
                and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
        ),
    {% endif %}
    valores_subsidio as (
        {% if is_disabled %}
            select *
            from sumario_servico_dia
            union all
        {% endif %}
        select *
        from sumario_faixa_servico_dia
    ),
    planejada as (
        select distinct data, consorcio, servico, vista
        from {{ ref("viagem_planejada") }}
        -- `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
        where
            data >= date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
            and (id_tipo_trajeto = 0 or id_tipo_trajeto is null)
            and format_time("%T", time(faixa_horaria_inicio)) != "00:00:00"
    ),
    pagamento as (
        select
            data,
            tipo_dia,
            consorcio,
            servico,
            vista,
            viagens_dia as viagens,
            km_apurada,
            km_planejada_dia as km_planejada,
            valor_a_pagar as valor_subsidio_pago,
            valor_penalidade
        from valores_subsidio as sdp
        left join planejada as p using (data, servico, consorcio)
        where
            data >= date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
            and data between date("{{ var('start_date') }}") and date_add(
                date("{{ var('end_date') }}"), interval 1 day
            )
    )
select
    data,
    tipo_dia,
    consorcio,
    servico,
    vista,
    viagens,
    km_apurada,
    km_planejada,
    round(100 * km_apurada / km_planejada, 2) as perc_km_planejada,
    valor_subsidio_pago,
    valor_penalidade
from pagamento
