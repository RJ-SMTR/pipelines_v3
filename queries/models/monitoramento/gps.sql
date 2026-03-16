{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        tags=["geolocalizacao"],
        alias=this.name ~ "_" ~ var("modo_gps") ~ "_" ~ var("fonte_gps"),
    )
}}

{% set staging_gps = ref("staging_gps") %}
{% if execute and is_incremental() %}
    {% set gps_partitions_query %}
    SELECT DISTINCT
      CONCAT("'", DATE(data), "'") AS data
    FROM
      {{ staging_gps }}
    WHERE
      {{ generate_date_hour_partition_filter(var('date_range_start'), var('date_range_end')) }}
    {% endset %}

    {% set gps_partitions = run_query(gps_partitions_query).columns[0].values() %}
{% endif %}

with
    registros as (
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
        select id_veiculo, datetime_gps, servico, tipo_parada
        from {{ ref("aux_gps_parada") }}
    ),
    indicadores as (
        select id_veiculo, datetime_gps, servico, indicador_trajeto_correto
        from {{ ref("aux_gps_trajeto_correto") }}
    ),
    novos_dados as (
        select
            date(r.datetime_gps) as data,
            extract(hour from r.datetime_gps) as hora,
            r.datetime_gps,
            r.id_veiculo,
            r.servico,
            r.latitude,
            r.longitude,
            case
                when
                    indicador_em_movimento is true and indicador_trajeto_correto is true
                then 'Em operação'
                when
                    indicador_em_movimento is true
                    and indicador_trajeto_correto is false
                then 'Operando fora trajeto'
                when indicador_em_movimento is false
                then
                    case
                        when tipo_parada is not null
                        then concat("Parado ", tipo_parada)
                        when indicador_trajeto_correto is true
                        then 'Parado trajeto correto'
                        else 'Parado fora trajeto'
                    end
            end as status,
            r.velocidade as velocidade_instantanea,
            v.velocidade as velocidade_estimada_10_min,
            v.distancia,
            0 as priority
        from registros r
        join indicadores i using (id_veiculo, datetime_gps, servico)
        join velocidades v using (id_veiculo, datetime_gps, servico)
        join paradas p using (id_veiculo, datetime_gps, servico)
    ),
    particoes_completas as (
        select *
        from novos_dados

        {% if is_incremental() and gps_partitions | length > 0 %}
            union all

            select * except (versao, datetime_ultima_atualizacao), 1 as priority
            from {{ this }}
            where data in ({{ gps_partitions | join(", ") }})
        {% endif %}
    )
select
    * except (priority),
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from particoes_completas
qualify
    row_number() over (partition by id_veiculo, datetime_gps, servico order by priority)
    = 1
