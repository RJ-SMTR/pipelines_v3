{{
    config(
        materialized="ephemeral",
    )
}}

with
    stops_rn as (
        select
            stop_id as id_servico,
            stop_code as servico,
            stop_name as descricao_servico,
            stop_lat as latitude,
            stop_lon as longitude,
            feed_start_date as inicio_vigencia,
            feed_end_date as fim_vigencia,
            lag(feed_end_date) over (
                partition by stop_id order by feed_start_date
            ) as feed_end_date_anterior,
            row_number() over (partition by stop_id order by feed_start_date desc) as rn
        from {{ ref("stops_gtfs") }}
        where location_type = '1'
    ),
    stops_agrupada as (
        select
            id_servico,
            inicio_vigencia,
            servico,
            descricao_servico,
            ifnull(fim_vigencia, current_date("America/Sao_Paulo")) as fim_vigencia,
            sum(
                case
                    when
                        feed_end_date_anterior is null
                        or feed_end_date_anterior
                        <> date_sub(inicio_vigencia, interval 1 day)
                    then 1
                    else 0
                end
            ) over (partition by id_servico order by inicio_vigencia) as group_id
        from stops_rn
    ),
    vigencia as (
        select
            id_servico,
            min(inicio_vigencia) as inicio_vigencia,
            max(fim_vigencia) as fim_vigencia
        from stops_agrupada
        group by id_servico, group_id
    )
select
    id_servico,
    r.servico,
    r.descricao_servico,
    r.latitude,
    r.longitude,
    v.inicio_vigencia,
    case
        when v.fim_vigencia != current_date("America/Sao_Paulo") then v.fim_vigencia
    end as fim_vigencia,
    'stops' as tabela_origem_gtfs,
from vigencia v
join
    (
        select id_servico, servico, descricao_servico, latitude, longitude
        from stops_rn
        where rn = 1
    ) r using (id_servico)
