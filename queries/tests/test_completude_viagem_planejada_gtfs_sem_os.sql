with
    calendario as (
        select data, feed_start_date, tipo_dia, service_ids
        from {{ ref("calendario") }}
        where
            data between date('{{ var("date_range_start") }}') and date(
                '{{ var("date_range_end") }}'
            )
    ),
    viagens_gtfs_sem_os as (
        select c.data, c.feed_start_date, vp.id_viagem
        from calendario c
        join
            {{ ref("viagem_planejada_planejamento") }} vp
            on c.feed_start_date = vp.feed_start_date
            and vp.service_id in unnest(c.service_ids)
            and vp.tipo_dia = c.tipo_dia
            and vp.tipo_os is null
    ),
    viagens_materializadas as (
        select data, feed_start_date, id_viagem
        from {{ ref("viagem_planejada_planejamento_dia") }}
        where
            data between date('{{ var("date_range_start") }}') and date(
                '{{ var("date_range_end") }}'
            )
    )
select e.data, e.feed_start_date, e.id_viagem
from viagens_gtfs_sem_os e
left join viagens_materializadas a using (data, feed_start_date, id_viagem)
where a.id_viagem is null
