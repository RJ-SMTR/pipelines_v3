{{
    config(
        materialized="table",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        tags=["geolocalizacao"],
        alias=this.name ~ "_" ~ var("modo_gps") ~ "_" ~ var("fonte_gps"),
    )
}}

with
    registros as (
        -- 1. registros_filtrada
        select
            data,
            hora,
            id_veiculo,
            datetime_gps,
            datetime_captura,
            velocidade,
            servico,
            latitude,
            longitude,
        from {{ ref("aux_gps_filtrada") }}
    ),
    velocidades as (
        -- 2. velocidades
        select
            id_veiculo,
            datetime_gps,
            servico,
            velocidade,
            distancia,
            indicador_em_movimento
        from {{ ref("aux_gps_velocidade") }}
    ),
    paradas as (
        -- 3. paradas
        select id_veiculo, datetime_gps, servico, tipo_parada
        from {{ ref("aux_gps_parada") }}
    ),
    indicadores as (
        -- 4. indicador_trajeto_correto
        select id_veiculo, datetime_gps, servico, indicador_trajeto_correto
        from {{ ref("aux_gps_trajeto_correto") }}
    )
-- 5. Junção final
select
    date(r.datetime_gps) data,
    extract(hour from r.datetime_gps) hora,
    r.datetime_gps,
    r.id_veiculo,
    r.servico,
    r.latitude,
    r.longitude,
    case
        when indicador_em_movimento is true and indicador_trajeto_correto is true
        then 'Em operação'
        when indicador_em_movimento is true and indicador_trajeto_correto is false
        then 'Operando fora trajeto'
        when indicador_em_movimento is false
        then
            case
                when tipo_parada is not null
                then concat("Parado ", tipo_parada)
                else
                    case
                        when indicador_trajeto_correto is true
                        then 'Parado trajeto correto'
                        else 'Parado fora trajeto'
                    end
            end
    end status,
    r.velocidade as velocidade_instantanea,
    v.velocidade as velocidade_estimada_10_min,
    v.distancia,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from registros r
join indicadores i using (id_veiculo, datetime_gps, servico)
join velocidades v using (id_veiculo, datetime_gps, servico)
join paradas p using (id_veiculo, datetime_gps, servico)
