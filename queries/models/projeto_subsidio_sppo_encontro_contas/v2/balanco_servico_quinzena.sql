{{
    config(
        partition_by={"field": "data_inicial_quinzena"},
    )
}}

with
    datas as (
        select
            data,
            case
                when extract(day from data) <= 15
                then format_date('%Y-%m-Q1', data)
                else format_date('%Y-%m-Q2', data)
            end as quinzena,
        from
            unnest(
                generate_date_array(
                    date("{{ var('start_date') }}"),
                    date("{{ var('end_date') }}"),
                    interval 1 day
                )
            ) as data
    ),
    quinzenas as (
        select
            quinzena,
            min(data) as data_inicial_quinzena,
            max(data) as data_final_quinzena
        from datas
        group by quinzena
    ),
    balanco_agg as (
        select
            quinzena,
            data_inicial_quinzena,
            data_final_quinzena,
            servico,
            consorcio,
            count(data) as quantidade_dias_subsidiado,
            sum(km_apurada) as km_apurada,
            sum(receita_total_esperada) as receita_total_esperada,
            sum(receita_tarifaria_esperada) as receita_tarifaria_esperada,
            sum(subsidio_esperado) as subsidio_esperado,
            sum(subsidio_glosado) as subsidio_glosado,
            sum(receita_total_aferida) as receita_total_aferida,
            sum(receita_tarifaria_aferida) as receita_tarifaria_aferida,
            sum(valor_subsidio_pago) as subsidio_pago,
            sum(saldo) as saldo
        from quinzenas qz
        left join
            {{ ref("balanco_servico_dia") }} bs
            on bs.data between qz.data_inicial_quinzena and qz.data_final_quinzena
        group by all
    )
select
    *,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from balanco_agg
