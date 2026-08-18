with
    servico_planejado_faixa_horaria as (
        select
            data,
            feed_start_date,
            tipo_dia,
            tipo_os,
            sum(partidas) as partidas,
            sum(quilometragem) as quilometragem
        from {{ ref("servico_planejado_faixa_horaria") }}
        where
            data between date('{{ var("date_range_start") }}') and date(
                '{{ var("date_range_end") }}'
            )
            and modo = 'Ônibus'
        group by all
    ),
    feeds as (select distinct feed_start_date from servico_planejado_faixa_horaria),
    ordem_servico_faixa_horaria_sentido as (
        select
            o.feed_start_date,
            o.tipo_dia,
            o.tipo_os,
            sum(o.partidas) as partidas,
            sum(o.quilometragem) as quilometragem
        from {{ ref("ordem_servico_faixa_horaria_sentido") }} as o
        inner join feeds as f using (feed_start_date)
        group by all
    )
select
    s.data,
    s.feed_start_date,
    s.tipo_dia,
    s.tipo_os,
    s.partidas as partidas_servico_planejado,
    o.partidas as partidas_ordem_servico,
    s.quilometragem as quilometragem_servico_planejado,
    o.quilometragem as quilometragem_ordem_servico
from servico_planejado_faixa_horaria as s
left join
    ordem_servico_faixa_horaria_sentido as o using (feed_start_date, tipo_dia, tipo_os)
where
    o.partidas is distinct from s.partidas
    or abs(o.quilometragem - s.quilometragem) > 0.1
