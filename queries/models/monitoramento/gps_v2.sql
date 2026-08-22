{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        tags=["geolocalizacao"],
        alias=this.name ~ "_" ~ var("modo_gps") ~ "_" ~ var("fonte_gps"),
        require_partition_filter=true,
    )
}}

{% set staging_gps_v2 = ref("staging_gps", v=2) %}
{% if execute and is_incremental() %}
    {% set gps_partitions_query %}
    select distinct concat("'", date(data), "'") as data
    from {{ staging_gps_v2 }}
    where
        {{ generate_date_hour_partition_filter(var("date_range_start"), var("date_range_end")) }}
    {% endset %}

    {% set gps_partitions = run_query(gps_partitions_query).columns[0].values() %}
{% endif %}

with
    registros as (
        select *
        from {{ ref("aux_gps_filtrada", v=2) }}
        where
            data between date("{{ var('date_range_start') }}") and date(
                "{{ var('date_range_end') }}"
            )
            and datetime_gps
            between datetime("{{ var('date_range_start') }}") and datetime(
                "{{ var('date_range_end') }}"
            )
    ),
    velocidades as (
        select id_registro, velocidade, distancia, indicador_em_movimento
        from {{ ref("aux_gps_velocidade", v=2) }}
    ),
    paradas as (select id_registro, tipo_parada from {{ ref("aux_gps_parada", v=2) }}),
    indicadores as (
        select id_registro, indicador_trajeto_correto
        from {{ ref("aux_gps_trajeto_correto", v=2) }}
    ),
    novos_dados as (
        select
            r.data,
            r.hora,
            r.id_registro,
            r.datetime_gps,
            r.datetime_envio,
            r.datetime_servidor,
            r.id_veiculo,
            r.id_equipamento,
            r.sequencial_equipamento,
            r.route_id,
            r.servico,
            r.id_viagem_planejada,
            r.trip_id,
            r.shape_id,
            r.direction_id,
            r.sentido,
            case
                when
                    v.indicador_em_movimento is true
                    and i.indicador_trajeto_correto is true
                then "Em operação"
                when
                    v.indicador_em_movimento is true
                    and i.indicador_trajeto_correto is false
                then "Operando fora trajeto"
                when v.indicador_em_movimento is false
                then
                    case
                        when p.tipo_parada is not null
                        then concat("Parado ", p.tipo_parada)
                        when i.indicador_trajeto_correto is true
                        then "Parado trajeto correto"
                        else "Parado fora trajeto"
                    end
            end as status,
            r.qualidade_sinal,
            r.fonte_posicao,
            r.fonte_velocidade,
            r.latitude,
            r.longitude,
            r.altitude,
            r.direcao,
            r.velocidade as velocidade_instantanea,
            v.velocidade as velocidade_estimada_10_min,
            v.distancia,
            r.quantidade_satelites,
            r.hdop,
            r.vdop,
            r.pdop,
            r.datetime_captura,
            0 as priority
        from registros r
        join indicadores i using (id_registro)
        join velocidades v using (id_registro)
        join paradas p using (id_registro)
    ),
    particoes_completas as (
        select *
        from novos_dados

        {% if is_incremental() and gps_partitions | length > 0 %}
            union all

            select
                data,
                hora,
                id_registro,
                datetime_gps,
                datetime_envio,
                datetime_servidor,
                id_veiculo,
                id_equipamento,
                sequencial_equipamento,
                route_id,
                servico,
                id_viagem_planejada,
                trip_id,
                shape_id,
                direction_id,
                sentido,
                status,
                qualidade_sinal,
                fonte_posicao,
                fonte_velocidade,
                latitude,
                longitude,
                altitude,
                direcao,
                velocidade_instantanea,
                velocidade_estimada_10_min,
                distancia,
                quantidade_satelites,
                hdop,
                vdop,
                pdop,
                datetime_captura,
                1 as priority
            from {{ this }}
            where data in ({{ gps_partitions | join(", ") }})
        {% endif %}
    )
select
    * except (priority),
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao
from particoes_completas
qualify row_number() over (partition by id_registro order by priority) = 1
