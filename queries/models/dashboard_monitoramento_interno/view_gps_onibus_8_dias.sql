{{
    config(
        materialized="view",
    )
}}

select distinct
    id_veiculo,
    servico,
    latitude,
    longitude,
    date(datetime_gps) as data,
    time(datetime_gps) as hora,
    datetime_gps as timestamp_gps
from {{ ref("view_gps_onibus") }}
where
    data between date_sub(current_date(), interval 8 day) and current_date()
    and status != "Parado garagem"
    and servico is not null
