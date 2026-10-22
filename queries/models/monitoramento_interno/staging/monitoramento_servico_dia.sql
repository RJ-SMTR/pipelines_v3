{% if var("start_date") >= var("DATA_SUBSIDIO_V9_INICIO") %}
    {{ config(enabled=false, materialized="ephemeral") }}
{% else %} {{ config(materialized="ephemeral") }}
{% endif %}
select
    data,
    tipo_dia,
    consorcio,
    servico,
    vista,
    viagens,
    km_apurada,
    km_planejada,
    perc_km_planejada,
    valor_subsidio_pago + coalesce(valor_penalidade, 0) as valor_subsidio_pago,
    coalesce(valor_penalidade, 0) as valor_penalidade
from {{ ref("sumario_servico_dia_historico") }}
-- `rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia_historico`
where
    data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")  -- noqa

    and data between date("{{ var('start_date') }}") and date_add(
        date("{{ var('end_date') }}"), interval 1 day
    )
