{% set sumario_servico_dia_historico = ref(
    "monitoramento_sumario_servico_dia_historico"
) %}
{# {% set sumario_servico_dia_historico = (
    "rj-smtr.monitoramento.sumario_servico_dia_historico"
) %} #}
select
    *,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from
    (
        select
            data,
            servico,
            consorcio,
            safe_cast(km_apurada as numeric) as km_apurada_pod,
            safe_cast(km_apurada as numeric) as km_apurada,
            safe_cast(km_planejada as numeric) as km_planejada,
            safe_cast(perc_km_planejada as numeric) as perc_km_planejada,
            safe_cast(valor_subsidio_pago as numeric) as valor_subsidio_pago
        from {{ sumario_servico_dia_historico }}
        where data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
        union all
        select *
        from {{ ref("aux_encontro_contas_servico_dia") }}
        where data >= date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}")
    )
where
    data >= "{{ var('encontro_contas_datas_v2_inicio') }}"
    and data between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
