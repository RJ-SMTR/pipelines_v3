{% macro gps_aux_parada(aux_gps_filtrada, versao=1) %}
    {% set incremental_filter %}
    data between
        date('{{ var("date_range_start") }}')
        and date('{{ var("date_range_end") }}')
    {% endset %}

    {% set calendario = ref("calendario") %}
    {% if execute %}
        {% set gtfs_feeds_query %}
        select distinct concat("'", feed_start_date, "'") as feed_start_date
        from {{ calendario }}
        where {{ incremental_filter }}
        {% endset %}
        {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
    {% endif %}

    with
        terminais as (
            select
                st_geogpoint(stop_lon, stop_lat) as ponto_parada,
                stop_name as nome_parada,
                'terminal' as tipo_parada
            from {{ ref("stops_gtfs") }}
            where
                location_type = "1"
                {# Lista vazia quando não há feed no período (ex.: execução com --empty) #}
                {% if gtfs_feeds | length > 0 %}
                    and feed_start_date in ({{ gtfs_feeds | join(", ") }})
                {% else %} and false
                {% endif %}
        ),
        garagens as (
            select
                * except (geometry_wkt),
                'garagem' as nome_parada,
                'garagem' as tipo_parada
            from {{ ref("garagem") }}
            where
                inicio_vigencia <= date('{{ var("date_range_end") }}')
                and (
                    fim_vigencia is null
                    or fim_vigencia >= date('{{ var("date_range_start") }}')
                )
        ),
        posicoes_veiculos as (
            select
                {% if versao == 2 %} id_registro, {% endif %}
                id_veiculo,
                datetime_gps,
                data,
                servico,
                posicao_veiculo_geo
            from {{ aux_gps_filtrada }}
            where
                data between date('{{ var("date_range_start") }}') and date(
                    '{{ var("date_range_end") }}'
                )
                and datetime_gps
                between datetime('{{ var("date_range_start") }}') and datetime(
                    '{{ var("date_range_end") }}'
                )
        ),
        veiculos_em_garagens as (
            select
                {% if versao == 2 %} v.id_registro, {% endif %}
                v.id_veiculo,
                v.datetime_gps,
                v.data,
                v.servico,
                v.posicao_veiculo_geo,
                g.nome_parada,
                g.tipo_parada
            from posicoes_veiculos v
            join
                garagens g
                on st_intersects(v.posicao_veiculo_geo, g.geometry)
                and v.data >= g.inicio_vigencia
                and (g.fim_vigencia is null or v.data <= g.fim_vigencia)
        ),
        terminais_proximos as (
            select
                {% if versao == 2 %} v.id_registro, {% endif %}
                v.id_veiculo,
                v.datetime_gps,
                v.data,
                v.servico,
                v.posicao_veiculo_geo,
                t.nome_parada,
                t.tipo_parada,
                round(
                    st_distance(v.posicao_veiculo_geo, t.ponto_parada), 1
                ) as distancia_parada,
                case
                    when
                        round(st_distance(v.posicao_veiculo_geo, t.ponto_parada), 1)
                        < {{ var("distancia_limiar_parada") }}
                    then t.tipo_parada
                    else null
                end as status_terminal
            from posicoes_veiculos v
            join
                terminais t
                on st_dwithin(
                    v.posicao_veiculo_geo,
                    t.ponto_parada,
                    {{ var("distancia_limiar_parada") }}
                )
            qualify
                row_number() over (
                    partition by
                        {% if versao == 2 %} v.id_registro
                        {% else %} v.datetime_gps, v.id_veiculo, v.servico
                        {% endif %}
                    order by st_distance(v.posicao_veiculo_geo, t.ponto_parada)
                )
                = 1
        ),
        resultados_combinados as (
            select
                {% if versao == 2 %} v.id_registro, {% endif %}
                v.id_veiculo,
                v.datetime_gps,
                v.data,
                v.servico,
                coalesce(g.tipo_parada, t.status_terminal) as tipo_parada,
                coalesce(g.nome_parada, t.nome_parada) as nome_parada
            from posicoes_veiculos v
            {% if versao == 2 %}
                left join veiculos_em_garagens g using (id_registro)
                left join terminais_proximos t using (id_registro)
            {% else %}
                left join
                    veiculos_em_garagens g
                    on v.id_veiculo = g.id_veiculo
                    and v.datetime_gps = g.datetime_gps
                left join
                    terminais_proximos t
                    on v.id_veiculo = t.id_veiculo
                    and v.datetime_gps = t.datetime_gps
            {% endif %}
        )
    select
        {% if versao == 2 %} id_registro, {% endif %}
        data,
        datetime_gps,
        id_veiculo,
        servico,
        tipo_parada,
        nome_parada
    from resultados_combinados
{% endmacro %}
