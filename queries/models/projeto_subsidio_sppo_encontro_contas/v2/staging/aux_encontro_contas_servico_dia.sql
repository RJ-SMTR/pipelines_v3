/*
- km_apurada_pod: soma das kms por tipo e por faixa horária que serão consideradas para o cálculo do POD (Percentual de Operação Diária) conforme rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem
    - Além das kms apuradas normalmente, considera também as kms não vistoriadas (2024-08-16 a 2024-09-30)
- km_apurada: soma das kms por faixa horária que foram apuradas e utilizadas para o cálculo do subsídio conforme tabelas rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia e rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia_pagamento
- km_planejada: soma das kms por faixa horária que foram planejadas e utilizadas para o cálculo do subsídio conforme tabelas rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia e rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia_pagamento
- perc_km_planejada: percentual de km_apurada_pod em relação a km_planejada, ou seja, o POD (Percentual de Operação Diária) do serviço
*/
{% set subsidio_faixa_servico_dia_tipo_viagem = ref(
    "subsidio_faixa_servico_dia_tipo_viagem"
) %}
{% set sumario_faixa_servico_dia = ref("sumario_faixa_servico_dia") %}
{% set sumario_faixa_servico_dia_pagamento = ref(
    "sumario_faixa_servico_dia_pagamento"
) %}
{# {% set subsidio_faixa_servico_dia_tipo_viagem = (
    "rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem"
) %} #}
{# {% set sumario_faixa_servico_dia = (
    "rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia"
) %} #}
{# {% set sumario_faixa_servico_dia_pagamento = (
    "rj-smtr.dashboard_subsidio_sppo_v2.sumario_faixa_servico_dia_pagamento"
) %} #}
{{ config(materialized="ephemeral") }}

with
    -- 1. Lista pares data-serviço com km_apurada_pod [soma das kms por tipo e por
    -- faixa horária que serão consideradas para o cálculo do POD (Percentual de
    -- Operação Diária) conforme
    -- rj-smtr.financeiro.subsidio_faixa_servico_dia_tipo_viagem. Além das kms
    -- apuradas normalmente, considera também as kms não vistoriadas (2024-08-16 a
    -- 2024-09-30)]
    sumario_servico_dia_pod as (
        select
            data,
            servico,
            safe_cast(
                sum(
                    case
                        when
                            data >= date("{{ var('DATA_SUBSIDIO_V9A_INICIO') }}")
                            and tipo_viagem not in ("Não licenciado", "Não vistoriado")
                        then km_apurada_faixa
                        when
                            data < date("{{ var('DATA_SUBSIDIO_V9A_INICIO') }}")
                            and tipo_viagem not in ("Não licenciado")
                        then km_apurada_faixa
                        else 0
                    end
                ) as numeric
            ) as km_apurada_pod,
        from {{ subsidio_faixa_servico_dia_tipo_viagem }}
        where
            data >= "{{ var('encontro_contas_datas_v2_inicio') }}"
            and data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
        group by all
    ),
    -- 2. Lista pares data-serviço junto à km_apurada_faixa e km_planejada_faixa
    -- sumario_faixa_servico_dia [2024-08-16 (apuração por faixa horária) a 2025-01-04
    -- (apuração por tecnologia)]
    -- sumario_faixa_servico_dia_pagamento [2025-01-05 (apuração por tecnologia) em
    -- diante]
    sumario_faixa_servico as (
        select data, consorcio, servico, km_apurada_faixa, km_planejada_faixa,
        from {{ sumario_faixa_servico_dia }}
        where
            data >= "{{ var('encontro_contas_datas_v2_inicio') }}"
            and data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
            and data < date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
        union all
        select data, consorcio, servico, km_apurada_faixa, km_planejada_faixa,
        from {{ sumario_faixa_servico_dia_pagamento }} as sdp
        where
            data >= "{{ var('encontro_contas_datas_v2_inicio') }}"
            and data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
            and data >= date("{{ var('DATA_SUBSIDIO_V14_INICIO') }}")
    ),
    -- 3. Agrupa pares data-serviço com km_apurada_faixa e km_planejada_faixa
    sumario_servico_dia as (
        select
            data,
            consorcio,
            servico,
            safe_cast(sum(km_apurada_faixa) as numeric) as km_apurada,
            safe_cast(sum(km_planejada_faixa) as numeric) as km_planejada,
        from sumario_faixa_servico
        group by all
    )
-- 4. Lista pares data-serviço e calcula perc_km_planejada
select
    data,
    servico,
    consorcio,
    km_apurada_pod,
    km_apurada,
    km_planejada,
    round(100 * km_apurada_pod / km_planejada, 2) as perc_km_planejada,
    null as valor_subsidio_pago,
from sumario_servico_dia as ssd
left join sumario_servico_dia_pod as ssdp using (data, servico)
