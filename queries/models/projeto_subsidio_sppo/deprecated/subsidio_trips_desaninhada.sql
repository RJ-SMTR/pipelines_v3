-- 1. Cria view das trips consideradas no subsidio
select
    trip_id,
    -- concat(substr(trip_id, 1, 10), substr(trip_id, 12, 4)) as trip_no_direction,
    route_id,
    trip_headsign,
    case
        when regexp_extract(trip_short_name, r'[A-Z]+') is null
        then trip_short_name
        else
            concat(
                regexp_extract(trip_short_name, r'[A-Z]+'),
                regexp_extract(trip_short_name, r'[0-9]+')
            )
    end as trip_short_name,
    shape_id,
    "DU" as variacao_itinerario,
    date(data_versao) data_versao
from {{ var("subsidio_trips") }} t
