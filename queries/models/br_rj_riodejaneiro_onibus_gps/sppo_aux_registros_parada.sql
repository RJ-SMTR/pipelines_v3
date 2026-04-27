{{ config(materialized="ephemeral") }}

/*
Descrição:
Identifica veículos parados em terminais ou garagens conhecidas.
1. Selecionamos os terminais conhecidos e uma geometria do tipo polígono (Polygon) que contém buracos nas
localizações das garagens.
2. Calculamos as distâncias do veículos em relação aos terminais conhecidos. Definimos aqui a coluna 'nrow',
que identifica qual o terminal que está mais próximo do ponto informado. No passo final, recuperamos apenas
os dados com nrow = 1 (menor distância em relação à posição do veículo)
3. Definimos uma distancia_limiar_parada. Caso o veículo esteja a uma distância menor que este valor de uma
parada, será considerado como parado no terminal com menor distancia.
4. Caso o veiculo não esteja intersectando o polígono das garagens, ele será considerado como parado dentro
de uma garagem (o polígono é vazado nas garagens, a não intersecção implica em estar dentro de um dos 'buracos').
*/
with
    terminais as (
        -- 1. Selecionamos terminais, criando uma geometria de ponto para cada.
        select
            st_geogpoint(longitude, latitude) ponto_parada,
            nome_terminal nome_parada,
            'terminal' tipo_parada
        from {{ var("sppo_terminais") }}
    ),
    garagem_polygon as (
        -- 1. Selecionamos o polígono das garagens.
        select st_geogfromtext(wkt, make_valid => true) as poly
        from {{ ref("garagens_polygon") }}
    ),
    distancia as (
        -- 2. Calculamos as distâncias e definimos nrow
        select
            id_veiculo,
            timestamp_gps,
            data,
            linha,
            posicao_veiculo_geo,
            nome_parada,
            tipo_parada,
            round(st_distance(posicao_veiculo_geo, ponto_parada), 1) distancia_parada,
            row_number() over (
                partition by timestamp_gps, id_veiculo, linha
                order by st_distance(posicao_veiculo_geo, ponto_parada)
            ) nrow
        from terminais p
        join
            (
                select id_veiculo, timestamp_gps, data, linha, posicao_veiculo_geo
                from {{ ref("sppo_aux_registros_filtrada") }}
                {% if not flags.FULL_REFRESH %}
                    where
                        data between date("{{var('date_range_start')}}") and date(
                            "{{var('date_range_end')}}"
                        )
                        and timestamp_gps > "{{var('date_range_start')}}"
                        and timestamp_gps <= "{{var('date_range_end')}}"
                {% endif %}
            ) r
            on 1 = 1
    )
select
    data,
    id_veiculo,
    timestamp_gps,
    linha,
    /*
  3. e 4. Identificamos o status do veículo como 'terminal', 'garagem' (para os veículos parados) ou
  null (para os veículos mais distantes de uma parada que o limiar definido)
  */
    case
        when distancia_parada < {{ var("distancia_limiar_parada") }}
        then tipo_parada
        when not st_intersects(posicao_veiculo_geo, (select poly from garagem_polygon))
        then 'garagem'
        else null
    end tipo_parada,
from distancia
where nrow = 1
