with
    servicos_ordem_servico as (
        select distinct feed_start_date, servico
        from {{ ref("ordem_servico_faixa_horaria_sentido") }}
        where feed_start_date = date('{{ var("data_versao_gtfs") }}') and partidas > 0
    ),
    trips_ordem_servico as (
        select t.feed_start_date, t.servico, t.trip_id
        from {{ ref("aux_trips") }} t
        inner join servicos_ordem_servico os using (feed_start_date, servico)
        where
            t.feed_start_date = date('{{ var("data_versao_gtfs") }}')
            and t.service_id != 'EXCEP'
    ),
    trips_com_frequencies as (
        select distinct feed_start_date, trip_id
        from {{ ref("aux_frequencies_horario_tratado") }}
        where feed_start_date = date('{{ var("data_versao_gtfs") }}')
    ),
    trips_com_stop_times as (
        select distinct feed_start_date, trip_id
        from {{ ref("aux_stop_times_horario_tratado") }}
        where
            feed_start_date = date('{{ var("data_versao_gtfs") }}')
            and stop_sequence = 0
    ),
    resultado as (
        select
            os.feed_start_date,
            os.servico,
            count(distinct t.trip_id) as quantidade_trips,
            count(
                distinct if(f.trip_id is not null, t.trip_id, null)
            ) as quantidade_trips_com_frequencies,
            count(
                distinct if(st.trip_id is not null, t.trip_id, null)
            ) as quantidade_trips_com_stop_times,
            count(
                distinct if(
                    t.trip_id is not null and f.trip_id is null and st.trip_id is null,
                    t.trip_id,
                    null
                )
            ) as quantidade_trips_sem_horario
        from servicos_ordem_servico os
        left join trips_ordem_servico t using (feed_start_date, servico)
        left join trips_com_frequencies f using (feed_start_date, trip_id)
        left join trips_com_stop_times st using (feed_start_date, trip_id)
        group by 1, 2
    )
select *
from resultado
where quantidade_trips = 0 or quantidade_trips_sem_horario > 0
