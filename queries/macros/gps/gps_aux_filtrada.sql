{% macro gps_aux_filtrada(staging_gps, staging_realocacao=none, versao=1) %}
    {% if execute and is_incremental() %}
        {% set gps_partitions = get_date_hour_partitions(staging_gps) %}
    {% endif %}

    with
        box as (
            /* Geometria de caixa que contém a área do município de Rio de Janeiro.*/
            select min_longitude, min_latitude, max_longitude, max_latitude
            from {{ ref("limites_geograficos_caixa") }}
        ),
        gps as (
            select *, st_geogpoint(longitude, latitude) posicao_veiculo_geo
            {% if versao == 2 %} from {{ staging_gps }}
            {% else %} from ({{ gps_aux_realocacao(staging_gps, staging_realocacao) }})
            {% endif %}
        ),
        filtrada as (
            select
                {% if versao == 2 %} gps.*
                {% else %}
                    data,
                    hora,
                    datetime_gps,
                    id_veiculo,
                    servico,
                    latitude,
                    longitude,
                    posicao_veiculo_geo,
                    datetime_captura,
                    velocidade,
                {% endif %}
            from gps
            cross join box
            where
                st_intersectsbox(
                    posicao_veiculo_geo,
                    min_longitude,
                    min_latitude,
                    max_longitude,
                    max_latitude
                )
        ),
        particoes_completas as (
            select *, 0 as priority
            from filtrada

            {% if is_incremental() and gps_partitions | length > 0 %}
                union all

                select *, 1 as priority
                from {{ this }}
                where data in ({{ gps_partitions | join(", ") }})
            {% endif %}
        )
    select * except (priority)
    from particoes_completas
    qualify
        row_number() over (
            partition by
                {% if versao == 2 %} id_registro
                {% else %} id_veiculo, datetime_gps
                {% endif %}
            order by priority
        )
        = 1
{% endmacro %}
