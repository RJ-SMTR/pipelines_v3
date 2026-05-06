{{
    config(
        materialized="view",
        tags=["geolocalizacao"],
    )
}}

with
    legado as (
        select
            data,
            extract(hour from timestamp_gps) as hora,
            timestamp_gps,
            id_veiculo,
            servico,
            latitude,
            longitude,
            status,
            velocidade_instantanea,
            velocidade_estimada_10_min,
            distancia,
            versao,
            cast(null as datetime) as datetime_ultima_atualizacao
        from {{ source("br_rj_riodejaneiro_veiculos", "gps_brt") }}
        where
            data <= date("{{ var('datetime_gps_brt_2') }}")
            and timestamp_gps < datetime("{{ var('datetime_gps_brt_2') }}")
    ),
    novo as (
        select
            data,
            hora,
            datetime_gps as timestamp_gps,
            id_veiculo,
            servico,
            latitude,
            longitude,
            status,
            velocidade_instantanea,
            velocidade_estimada_10_min,
            distancia,
            versao,
            datetime_ultima_atualizacao
        from {{ source("monitoramento", "gps_brt_sonda") }}
        where
            data >= date("{{ var('datetime_gps_brt_2') }}")
            and datetime_gps >= datetime("{{ var('datetime_gps_brt_2') }}")
    )
select *
from legado
union all
select *
from novo
