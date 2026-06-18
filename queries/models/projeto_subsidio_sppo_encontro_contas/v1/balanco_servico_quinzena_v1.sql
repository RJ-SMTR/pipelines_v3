{% if var("start_date") >= var("DATA_SUBSIDIO_V9_INICIO") %} {{ config(enabled=false) }}
{% else %} {{ config(alias=this.name ~ var("encontro_contas_modo")) }}
{% endif %}

with
    q1 as (
        select
            format_date('%Y-%m-Q1', date) as quinzena,
            date as data_inicial_quinzena,
            date_add(date, interval 14 day) as data_final_quinzena
        from
            unnest(
                generate_date_array('2022-06-01', '2023-12-31', interval 1 month)
            ) as date
    ),
    q2 as (
        select
            format_date('%Y-%m-Q2', date) as quinzena,
            date_add(date, interval 15 day) as data_inicial_quinzena,
            last_day(date) as data_final_quinzena
        from
            unnest(
                generate_date_array('2022-06-01', '2023-12-31', interval 1 month)
            ) as date
    ),
    quinzenas as (
        select *
        from q1
        union all
        select *
        from q2
        order by data_inicial_quinzena
    )
select
    quinzena,
    data_inicial_quinzena,
    data_final_quinzena,
    consorcio,
    servico,
    count(data) as quantidade_dias_subsidiado,
    sum(km_subsidiada) as km_subsidiada,
    sum(receita_total_esperada) as receita_total_esperada,
    sum(receita_tarifaria_esperada) as receita_tarifaria_esperada,
    sum(subsidio_esperado) as subsidio_esperado,
    sum(subsidio_glosado) as subsidio_glosado,
    sum(receita_total_aferida) as receita_total_aferida,
    sum(receita_tarifaria_aferida) as receita_tarifaria_aferida,
    sum(subsidio_pago) as subsidio_pago,
    sum(saldo) as saldo
from quinzenas qz
left join
    {{ ref("balanco_servico_dia" ~ var("encontro_contas_modo"), v=1) }} bs
    on bs.data between qz.data_inicial_quinzena and qz.data_final_quinzena
group by quinzena, data_inicial_quinzena, data_final_quinzena, consorcio, servico
order by data_inicial_quinzena, consorcio, servico
