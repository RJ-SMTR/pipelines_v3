{% if var("encontro_contas_modo") == "" and var("start_date") < var(
    "DATA_SUBSIDIO_V9_INICIO"
) %}
    {{ config(alias=this.name ~ var("encontro_contas_modo")) }}
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
        consorcio_rdo,
        servico_tratado_rdo,
        linha_rdo,
        tipo_servico_rdo,
        ordem_servico_rdo,
        count(data_rdo) as quantidade_dias_rdo,
        sum(receita_tarifaria_aferida_rdo) as receita_tarifaria_aferida_rdo
    from quinzenas qz
    left join
        (
            select * from {{ ref("aux_balanco_rdo_servico_dia") }} where servico is null
        ) bs
        on bs.data_rdo between qz.data_inicial_quinzena and qz.data_final_quinzena
    group by 1, 2, 3, 4, 5, 6, 7, 8
    order by 2, 4, 5, 6, 7, 8
{% else %} {{ config(enabled=false) }}
{% endif %}
