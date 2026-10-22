{{
    config(
        materialized="table",
    )
}}

with
    gps_agregado as (
        select
            data,
            operadora,
            id_validador,
            latitude,
            longitude,
            estado_equipamento,
            primeiro_datetime_gps,
            ultimo_datetime_gps,
            timestamp_diff(ultimo_datetime_gps, primeiro_datetime_gps, minute)
            + 1 as qtde_min_entre_a_prim_e_ultima_transmissao,
            count(*) over (partition by operadora, id_validador) as qtde_registros_gps,
            count(distinct format_timestamp("%F %H:%M", datetime_gps)) over (
                partition by operadora, id_validador
            ) as qtde_min_distintos_houve_transmissao,
            sum(
                case
                    when
                        latitude != 0
                        and longitude != 0
                        and latitude is not null
                        and longitude is not null
                    then 1
                    else 0
                end
            ) over (partition by operadora, id_validador)
            as qtde_registros_gps_georreferenciados,
            row_number() over (
                partition by operadora, id_validador order by datetime_gps
            ) as rn
        from
            (
                select
                    *,
                    min(datetime_gps) over (
                        partition by operadora, id_validador
                    ) as primeiro_datetime_gps,
                    max(datetime_gps) over (
                        partition by operadora, id_validador
                    ) as ultimo_datetime_gps,
                    row_number() over (
                        partition by id_transmissao_gps order by datetime_captura desc
                    ) as rn
                from {{ ref("gps_validador") }}
                where data = current_date("America/Sao_Paulo") and modo = "Ônibus"
            )
        where rn = 1
    )
select
    operadora,
    id_validador,
    latitude,
    longitude,
    data,
    estado_equipamento,
    primeiro_datetime_gps,
    ultimo_datetime_gps,
    qtde_min_entre_a_prim_e_ultima_transmissao,
    qtde_min_distintos_houve_transmissao,
    qtde_registros_gps,
    qtde_registros_gps_georreferenciados,
    ifnull(
        safe_divide(qtde_registros_gps_georreferenciados, qtde_registros_gps), 0
    ) as percentual_registros_gps_georreferenciados,
    ifnull(
        safe_divide(
            qtde_min_distintos_houve_transmissao,
            qtde_min_entre_a_prim_e_ultima_transmissao
        ),
        0
    ) as percentual_transmissao_a_cada_min
from gps_agregado
where rn = 1
