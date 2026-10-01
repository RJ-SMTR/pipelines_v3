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
    /*
    GPS de bordo SPPO usa prefixo (C47476); Jaé manda só a ordem (47476).
    Maxtrack (GTU/TUSA) usa A2-/B2-; hardcode das operadoras 2801/2802,
    mesmo padrão do gps_viagem.
    2801 - GTU (A2)
    2802 - TUSA (B2)
    */
    veiculos_bordo as (
        select
            data,
            substr(id_veiculo, 2) as id_veiculo_join,
            any_value(id_veiculo) as id_veiculo
        from gps_bordo
        group by data, id_veiculo_join
    ),
    gps_validador_filtrado as (
        select
            data,
            datetime_gps,
            case
                when id_operadora = "2801"
                then "A2-" || lpad(right(id_veiculo, 3), 3, "0")
                when id_operadora = "2802"
                then "B2-" || lpad(right(id_veiculo, 3), 3, "0")
                else id_veiculo
            end as id_veiculo,
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
            and id_veiculo != "99999"
            and modo = "Ônibus"
            and servico_jae is not null
            and latitude is not null
            and longitude is not null
            and latitude != 0
            and longitude != 0
    ),
    gps_validador as (
        select
            j.data,
            j.datetime_gps,
            coalesce(b.id_veiculo, j.id_veiculo) as id_veiculo,
            j.servico,
            j.latitude,
            j.longitude,
            j.fonte_gps,
            j.status,
            j.distancia
        from gps_validador_filtrado as j
        left join
            veiculos_bordo as b on j.data = b.data and j.id_veiculo = b.id_veiculo_join
    )
select *
from gps_bordo

union all

select *
from gps_validador
