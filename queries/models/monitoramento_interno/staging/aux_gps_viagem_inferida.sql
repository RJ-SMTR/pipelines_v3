{{ config(materialized="ephemeral") }}

{% set gps_data_filter %}
    data between date('{{ var("date_range_start") }}') and date_add(
        date('{{ var("date_range_end") }}'), interval 1 day
    )
    and datetime_gps between datetime_trunc(
        date('{{ var("date_range_start") }}'), day
    ) and datetime_add(
        datetime_trunc(
            date_add(date('{{ var("date_range_end") }}'), interval 1 day), day
        ),
        interval 3 hour
    )
{% endset %}

with
    gps_onibus as (
        select
            data,
            datetime_gps,
            id_veiculo,
            trim(servico) as servico,
            latitude,
            longitude,
            fonte_gps,
            status,
            distancia
        from {{ ref("view_gps_onibus") }}
        where
            {{ gps_data_filter }}
            and status != "Parado garagem"
            and latitude is not null
            and longitude is not null
    ),
    gps_brt as (
        select
            data,
            datetime_gps,
            id_veiculo,
            servico,
            latitude,
            longitude,
            fonte_gps,
            status,
            distancia
        from
            (
                select
                    data,
                    timestamp_gps as datetime_gps,
                    id_veiculo,
                    trim(servico) as servico,
                    latitude,
                    longitude,
                    "sonda" as fonte_gps,
                    status,
                    distancia
                from {{ ref("view_gps_brt_completo") }}
            ) as gps_brt_raw
        where
            {{ gps_data_filter }}
            and ifnull(status, "") != "Parado garagem"
            and latitude is not null
            and longitude is not null
    ),
    gps_bordo as (
        select *
        from gps_onibus

        union all

        select *
        from gps_brt
    ),
    veiculos_bordo as (select distinct data, id_veiculo from gps_bordo),
    gps_validador_filtrado as (
        select
            data,
            datetime_gps,
            id_veiculo,
            trim(servico_jae) as servico,
            latitude,
            longitude,
            "jae" as fonte_gps,
            cast(null as string) as status,
            cast(null as float64) as distancia
        from {{ ref("gps_validador") }}
        where
            {{ gps_data_filter }}
            and id_veiculo is not null
            and modo = "Ônibus"
            and servico_jae is not null
            and latitude is not null
            and longitude is not null
    ),
    gps_validador as (
        select j.*
        from gps_validador_filtrado as j
        left join veiculos_bordo as b using (data, id_veiculo)
        where b.id_veiculo is null
    )
select *
from gps_bordo

union all

select *
from gps_validador
