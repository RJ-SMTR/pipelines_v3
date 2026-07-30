{% set indicadores_false %}
not (indicador_atipico or indicador_ausencia_receita_tarifaria or indicador_data_excecao)
{% endset %}

with
    -- 1. Lista os pares data-serviço subsidiados e com receita tarifária
    -- Se não há receita tarifária, não entra para o Encontro de Contas, mas os pares
    -- permanecem (com valores nulos para posterior quantificação)
    balanco_rdo_subsidio_servico_dia as (
        select
            data,
            servico,
            consorcio,
            if(
                {{ indicadores_false }}, receita_tarifaria_aferida, null
            ) as receita_tarifaria_aferida,
            if({{ indicadores_false }}, km_apurada, null) as km_apurada,
            if(
                {{ indicadores_false }}, valor_subsidio_pago, null
            ) as valor_subsidio_pago,
            datas_servico,
            datas_servico_pod_maior_80,
            datas_servico_pod_menor_80,
            indicador_atipico,
            indicador_data_excecao,
            indicador_ausencia_receita_tarifaria
        from {{ ref("aux_balanco_rdo_subsidio_servico_dia") }}
    ),
    -- 2. Lista os parâmetros de subsídio
    parametros as (select * from {{ ref("encontro_contas_parametros") }})
select
    *,
    if(
        data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
        (
            ifnull(receita_total_aferida, 0)
            - ifnull(receita_total_esperada - subsidio_glosado, 0)
        ),
        (ifnull(receita_tarifaria_aferida, 0) - ifnull(receita_tarifaria_esperada, 0))
    ) as saldo
from
    (
        select
            b.* except (valor_subsidio_pago, receita_tarifaria_aferida),
            if(
                b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
                b.km_apurada * par.irk,
                null
            ) as receita_total_esperada,
            b.km_apurada * par.irk_tarifa_publica as receita_tarifaria_esperada,
            if(
                b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
                b.km_apurada * par.subsidio_km,
                null
            ) as subsidio_esperado,
            if(
                b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
                (b.km_apurada * par.subsidio_km - valor_subsidio_pago),
                null
            ) as subsidio_glosado,
            if(
                b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
                ifnull(b.receita_tarifaria_aferida, 0)
                + ifnull(b.valor_subsidio_pago, 0),
                null
            ) as receita_total_aferida,
            b.receita_tarifaria_aferida,
            if(
                b.data < date("{{ var('DATA_SUBSIDIO_V9_INICIO') }}"),
                b.valor_subsidio_pago,
                null
            ) as valor_subsidio_pago
        from balanco_rdo_subsidio_servico_dia as b
        left join parametros as par on b.data between data_inicio and data_fim
    )
