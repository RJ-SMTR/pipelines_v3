{{ config(materialized="view", alias="gps_15_minutos") }}

with
    gps_conecta as (
        select data, timestamp_gps, servico, id_veiculo, latitude, longitude
        {# from `rj-smtr.monitoramento.gps_15_minutos_onibus_conecta` #}
        from {{ source("monitoramento", "gps_15_minutos_onibus_conecta") }}
    ),
    gps_zirix as (
        select data, timestamp_gps, servico, id_veiculo, latitude, longitude
        {# from `rj-smtr.monitoramento.gps_15_minutos_onibus_zirix` #}
        from {{ source("monitoramento", "gps_15_minutos_onibus_zirix") }}
    ),
    gps_cittati as (
        select data, timestamp_gps, servico, id_veiculo, latitude, longitude
        {# from `rj-smtr.monitoramento.gps_15_minutos_onibus_cittati` #}
        from {{ source("monitoramento", "gps_15_minutos_onibus_cittati") }}
    ),
    gps_union as (
        select *, 'conecta' as fonte_gps
        from gps_conecta

        union all

        select *, 'zirix' as fonte_gps
        from gps_zirix

        union all

        select *, 'cittati' as fonte_gps
        from gps_cittati
    )
select *
from gps_union
