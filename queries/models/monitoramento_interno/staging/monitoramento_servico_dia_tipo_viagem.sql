{% if var("start_date") >= var("DATA_SUBSIDIO_V9_INICIO") %} {{ config(enabled=false) }}
{% else %} {{ config(materialized="ephemeral") }}
{% endif %}

select
    data,
    tipo_dia,
    consorcio,
    servico,
    tipo_viagem,
    indicador_ar_condicionado,
    viagens,
    km_apurada
from {{ ref("sumario_servico_tipo_viagem_dia") }}
-- `rj-smtr.dashboard_subsidio_sppo.sumario_servico_tipo_viagem_dia`
where
    data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
    {% if is_incremental() %}
        and data between date("{{var('start_date')}}") and date_add(
            date("{{ var('end_date') }}"), interval 1 day
        )
    {% endif %}
