-- depends_on: {{ ref('ordem_servico_trajeto_alternativo_gtfs') }}
-- depends_on: {{ ref('ordem_servico_trajeto_alternativo_sentido') }}
/*
Identificação de um trip de referência para cada serviço e sentido regular
Identificação de todas as trips de referência para os trajetos alternativos
*/
{{ config(materialized="ephemeral") }}

{% if execute -%}
    {%- set query = (
        "SELECT DISTINCT evento FROM "
        ~ ref("ordem_servico_trajeto_alternativo_gtfs")
        ~ " WHERE feed_start_date = '"
        ~ var("data_versao_gtfs")
        ~ "'"
        ~ " UNION DISTINCT"
        ~ " SELECT DISTINCT evento FROM "
        ~ ref("ordem_servico_trajeto_alternativo_sentido")
        ~ " WHERE feed_start_date = '"
        ~ var("data_versao_gtfs")
        ~ "'"
    ) -%}
    {%- set eventos_trajetos_alternativos = run_query(query).columns[0].values() -%}
{% endif %}

with
    -- 1. Busca os shapes em formato geográfico
    shapes as (
        select *
        from {{ ref("shapes_geom_gtfs") }}
        where feed_start_date = '{{ var("data_versao_gtfs") }}'
    ),
    -- 2. Busca as trips
    trips_all as (
        select
            *,
            case
                when indicador_trajeto_alternativo = true
                then
                    concat(
                        feed_version, trip_short_name, tipo_dia, direction_id, shape_id
                    )
                else concat(feed_version, trip_short_name, tipo_dia, direction_id)
            end as trip_partition
        from
            (
                select
                    service_id,
                    trip_id,
                    trip_headsign,
                    trip_short_name,
                    direction_id,
                    shape_id,
                    feed_version,
                    shape_distance,
                    start_pt,
                    end_pt,
                    case
                        when service_id like "%U_%"
                        then "Dia Útil"
                        when service_id like "%S_%"
                        then "Sabado"
                        when service_id like "%D_%"
                        then "Domingo"
                        else service_id
                    end as tipo_dia,
                    case
                        when
                            (
                                {% for evento in eventos_trajetos_alternativos %}
                                    trip_headsign like "%{{evento}}%" or
                                {% endfor %} service_id
                                = "EXCEP"
                            )
                        then true
                        else false
                    end as indicador_trajeto_alternativo,
                from {{ ref("trips_gtfs") }}
                left join shapes using (feed_start_date, feed_version, shape_id)
                where
                    feed_start_date = '{{ var("data_versao_gtfs") }}'
                    and service_id not like "%_DESAT_%"  -- Desconsidera service_ids desativados
            )
    )
-- 3. Busca as trips de referência para cada serviço, sentido, e tipo_dia
select * except (shape_distance),
from trips_all
qualify
    row_number() over (
        partition by trip_partition
        order by
            feed_version, trip_short_name, tipo_dia, direction_id, shape_distance desc
    )
    = 1
