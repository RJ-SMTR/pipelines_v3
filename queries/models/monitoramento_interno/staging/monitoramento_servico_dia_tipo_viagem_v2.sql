{{ config(materialized="ephemeral") }}

select
    data,
    tipo_dia,
    consorcio,
    servico,
    tipo_viagem,
    indicador_ar_condicionado,
    sum(viagens_faixa) as viagens,
    sum(km_apurada_faixa) as km_apurada
from {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }}
-- `rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem`
where
    data >= date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
    and tipo_viagem != "Sem viagem apurada"
    {% if is_incremental() %}
        and data between date("{{var('start_date')}}") and date_add(
            date("{{ var('end_date') }}"), interval 1 day
        )
    {% endif %}
group by data, tipo_dia, consorcio, servico, tipo_viagem, indicador_ar_condicionado
