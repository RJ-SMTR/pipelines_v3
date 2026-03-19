{{ config(materialized="ephemeral") }}

with
    anterior as (
        select
            data,
            hora,
            id_veiculo,
            datetime_gps,
            servico,
            latitude,
            longitude,
            posicao_veiculo_geo,
            datetime_captura,
            velocidade as velocidade_original,
            lag(posicao_veiculo_geo) over w as posicao_anterior,
            lag(datetime_gps) over w as datetime_anterior
        from {{ ref("aux_gps_filtrada") }}
        window w as (partition by id_veiculo, servico order by datetime_gps)
    ),
    calculo_velocidade as (
        select
            *,
            st_distance(posicao_veiculo_geo, posicao_anterior) as distancia,
            ifnull(
                safe_divide(
                    st_distance(posicao_veiculo_geo, posicao_anterior),
                    datetime_diff(datetime_gps, datetime_anterior, second)
                ),
                0
            )
            * 3.6 as velocidade
        from anterior
    ),
    media as (
        select
            data,
            datetime_gps,
            id_veiculo,
            servico,
            distancia,
            velocidade,
            -- Calcula média móvel
            avg(velocidade) over (
                partition by id_veiculo, servico
                order by
                    unix_seconds(timestamp(datetime_gps))
                    range
                    between {{ var("janela_movel_velocidade") }} preceding
                    and current row
            ) as velocidade_media
        from calculo_velocidade
    )
select
    data,
    datetime_gps,
    id_veiculo,
    servico,
    distancia,
    round(
        case
            when velocidade_media > {{ var("velocidade_maxima") }}
            then {{ var("velocidade_maxima") }}
            else velocidade_media
        end,
        1
    ) as velocidade,
    case
        when velocidade_media < {{ var("velocidade_limiar_parado") }}
        then false
        else true
    end as indicador_em_movimento
from media
