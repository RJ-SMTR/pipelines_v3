{{
    config(
        description="As partidas e a quilometragem das viagens planejadas do SPPO no GTFS correspondem às da Ordem de Serviço para cada feed_start_date, tipo_dia, tipo_os, servico, sentido e faixa horária."
    )
}}

{% set feed_filter %}
    feed_start_date = date('{{ var("data_versao_gtfs") }}')
{% endset %}

with
    viagens as (
        select
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            sentido,
            horario_partida,
            extensao
        from {{ ref("viagem_planejada_planejamento") }}
        where {{ feed_filter }} and sistema = 'SPPO'
    ),
    ordem_servico as (
        select
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            left(sentido, 1) as sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            partidas,
            quilometragem
        from {{ ref("ordem_servico_faixa_horaria_sentido") }}
        where {{ feed_filter }}
    ),
    /* Horários no formato HH:MM:SS permitem comparar como string. Viagens fora de qualquer faixa da OS ficam com faixa nula e aparecem como divergência */
    viagens_faixa as (
        select
            v.feed_start_date,
            v.tipo_dia,
            v.tipo_os,
            v.servico,
            v.sentido,
            o.faixa_horaria_inicio,
            o.faixa_horaria_fim,
            count(*) as partidas,
            sum(v.extensao) as quilometragem
        from viagens as v
        left join
            ordem_servico as o
            on v.feed_start_date = o.feed_start_date
            and v.tipo_dia = o.tipo_dia
            and v.tipo_os = o.tipo_os
            and v.servico = o.servico
            and v.sentido = o.sentido
            and v.horario_partida between o.faixa_horaria_inicio and o.faixa_horaria_fim
        group by all
    )
select
    feed_start_date,
    tipo_dia,
    tipo_os,
    servico,
    sentido,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    v.partidas as partidas_gtfs,
    o.partidas as partidas_ordem_servico,
    v.quilometragem as quilometragem_gtfs,
    o.quilometragem as quilometragem_ordem_servico
from viagens_faixa as v
full outer join
    ordem_servico as o using (
        feed_start_date,
        tipo_dia,
        tipo_os,
        servico,
        sentido,
        faixa_horaria_inicio,
        faixa_horaria_fim
    )
where
    (
        coalesce(v.partidas, 0) != coalesce(o.partidas, 0)
        or abs(coalesce(v.quilometragem, 0) - coalesce(o.quilometragem, 0)) > 0.1
    )
    /* Feeds das eleições: OS sem partidas em Dia Útil, Sábado e Ponto Facultativo */
    and feed_start_date not in ('2026-10-04', '2026-10-25')
    /* Exceção aprovada: o GTFS de Ponto Facultativo destes feeds diverge da OS */
    and not (
        feed_start_date in ('2026-10-24', '2026-10-26')
        and tipo_dia = 'Ponto Facultativo'
    )
