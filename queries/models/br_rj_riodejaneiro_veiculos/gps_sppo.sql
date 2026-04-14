{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        tags=["geolocalizacao"],
        require_partition_filter=true,
    )
}}
/*
Descrição:
Junção dos passos de tratamento, junta as informações extras que definimos a partir dos registros
capturados.
Para descrição detalhada de como cada coluna é calculada, consulte a documentação de cada uma das tabelas
utilizadas abaixo.
1. registros_filtrada: filtragem e tratamento básico dos dados brutos capturados.
2. aux_registros_velocidade: estimativa da velocidade de veículo a cada ponto registrado e identificação
do estado de movimento ('parado', 'andando')
3. aux_registros_parada: identifica veículos parados em terminais ou garagens conhecidas
4. aux_registros_flag_trajeto_correto: calcula intersecções das posições registradas para cada veículo
com o traçado da linha informada.
5. As junções (joins) são feitas sobre o id_veículo e a timestamp_gps.
*/
with
    registros as (
        -- 1. registros_filtrada
        select
            id_veiculo,
            timestamp_gps,
            timestamp_captura,
            velocidade,
            linha,
            latitude,
            longitude,
        from {{ ref("sppo_aux_registros_filtrada") }}
        {% if is_incremental() -%}
            where
                data between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
                and timestamp_gps > "{{var('date_range_start')}}"
                and timestamp_gps <= "{{var('date_range_end')}}"
        {%- endif -%}
    ),
    velocidades as (
        -- 2. velocidades
        select
            id_veiculo, timestamp_gps, linha, velocidade, distancia, flag_em_movimento
        from {{ ref("sppo_aux_registros_velocidade") }}
    ),
    paradas as (
        -- 3. paradas
        select id_veiculo, timestamp_gps, linha, tipo_parada,
        from {{ ref("sppo_aux_registros_parada") }}
    ),
    flags as (
        -- 4. flag_trajeto_correto
        select
            id_veiculo,
            timestamp_gps,
            linha,
            route_id,
            flag_linha_existe_sigmob,
            flag_trajeto_correto,
            flag_trajeto_correto_hist
        from {{ ref("sppo_aux_registros_flag_trajeto_correto") }}
    )
-- 5. Junção final
select
    "SPPO" modo,
    r.timestamp_gps,
    date(r.timestamp_gps) data,
    extract(time from r.timestamp_gps) hora,
    r.id_veiculo,
    r.linha as servico,
    r.latitude,
    r.longitude,
    case
        when flag_em_movimento is true and flag_trajeto_correto_hist is true
        then true
        else false
    end flag_em_operacao,
    v.flag_em_movimento,
    p.tipo_parada,
    flag_linha_existe_sigmob,
    flag_trajeto_correto,
    flag_trajeto_correto_hist,
    case
        when flag_em_movimento is true and flag_trajeto_correto_hist is true
        then 'Em operação'
        when flag_em_movimento is true and flag_trajeto_correto_hist is false
        then 'Operando fora trajeto'
        when flag_em_movimento is false
        then
            case
                when tipo_parada is not null
                then concat("Parado ", tipo_parada)
                else
                    case
                        when flag_trajeto_correto_hist is true
                        then 'Parado trajeto correto'
                        else 'Parado fora trajeto'
                    end
            end
    end status,
    r.velocidade velocidade_instantanea,
    v.velocidade velocidade_estimada_10_min,
    v.distancia,
    "{{ var('version') }}" as versao
from registros r

join
    flags f
    on r.id_veiculo = f.id_veiculo
    and r.timestamp_gps = f.timestamp_gps
    and r.linha = f.linha

join
    velocidades v
    on r.id_veiculo = v.id_veiculo
    and r.timestamp_gps = v.timestamp_gps
    and r.linha = v.linha

join
    paradas p
    on r.id_veiculo = p.id_veiculo
    and r.timestamp_gps = p.timestamp_gps
    and r.linha = p.linha
{% if is_incremental() -%}
    where
        date(r.timestamp_gps) between date("{{var('date_range_start')}}") and date(
            "{{var('date_range_end')}}"
        )
        and r.timestamp_gps > "{{var('date_range_start')}}"
        and r.timestamp_gps <= "{{var('date_range_end')}}"
{%- endif -%}
