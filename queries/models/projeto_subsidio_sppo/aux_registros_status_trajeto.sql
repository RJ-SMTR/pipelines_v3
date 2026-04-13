-- depends_on: {{ ref('subsidio_data_versao_efetiva') }}
{% if execute %}
    {% set result = run_query(
        "SELECT feed_start_date FROM "
        ~ ref("subsidio_data_versao_efetiva")
        ~ " WHERE data BETWEEN DATE_SUB(DATE('"
        ~ var("run_date")
        ~ "'), INTERVAL 2 DAY) AND DATE_SUB(DATE('"
        ~ var("run_date")
        ~ "'), INTERVAL 1 DAY)"
    ) %}
    {% set feed_start_dates = result.columns[0].values() %}
{% endif %}

{% if var("run_date") == "2024-05-05" %}
    -- Apuração "Madonna · The Celebration Tour in Rio"
    {% set gps_interval = 7 %}
{% else %} {% set gps_interval = 3 %}
{% endif %}

{% if var("run_date") < var("DATA_SUBSIDIO_V9_INICIO") %}

    -- 1. Seleciona sinais de GPS registrados no período
    with
        gps as (
            select
                g.* except (longitude, latitude, servico),
                {% if var("run_date") > "2023-01-16" and var(
                    "run_date"
                ) < "2023-12-02" %}
                    -- Substitui servicos noturnos por regulares, salvo exceções
                    case
                        when
                            servico like "SN%"
                            and servico not in ("SN006", "SN415", "SN474", "SN483")
                        then regexp_extract(servico, r'[0-9]+')
                        else servico
                    end as servico,
                {% else %} servico,
                {% endif %}
                substr(id_veiculo, 2, 3) as id_empresa,
                st_geogpoint(longitude, latitude) posicao_veiculo_geo,
                {% if var("run_date") > var("DATA_SUBSIDIO_V6_INICIO") %}
                    date_sub(
                        date("{{ var(" run_date ") }}"), interval 1 day
                    ) as data_operacao
                {% endif %}
            from
                -- `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo` g
                {{ ref("gps_sppo") }} g
            where
                (
                    data between date_sub(
                        date("{{ var(" run_date ") }}"), interval 1 day
                    ) and date("{{ var(" run_date ") }}")
                )
                -- Limita range de busca do gps de D-2 às 00h até D-1 às 3h
                and (
                    timestamp_gps
                    between datetime_sub(
                        datetime_trunc("{{ var(" run_date ") }}", day),
                        interval 1 day
                    ) and datetime_add(
                        datetime_trunc("{{ var(" run_date ") }}", day),
                        interval {{ gps_interval }} hour
                    )
                )
                and status != "Parado garagem"
                {% if var("run_date") == "2024-05-05" %}
                    -- Apuração "Madonna · The Celebration Tour in Rio"
                    and servico != "SE001"
                {% endif %}
        ),
        -- 2. Classifica a posição do veículo em todos os shapes possíveis de
        -- serviços de uma mesma empresa
        status_viagem as (
            select
                {% if var("run_date") > var("DATA_SUBSIDIO_V6_INICIO") %}
                    data_operacao as data,
                {% else %} g.data,
                {% endif %}
                g.id_veiculo,
                g.id_empresa,
                g.timestamp_gps,
                timestamp_trunc(g.timestamp_gps, minute) as timestamp_minuto_gps,
                g.posicao_veiculo_geo,
                trim(g.servico, " ") as servico_informado,
                s.servico as servico_realizado,
                s.shape_id,
                s.sentido_shape,
                s.shape_id_planejado,
                s.trip_id,
                s.trip_id_planejado,
                s.sentido,
                s.start_pt,
                s.end_pt,
                s.distancia_planejada,
                ifnull(g.distancia, 0) as distancia,
                case
                    when
                        st_dwithin(g.posicao_veiculo_geo, start_pt, {{ var("buffer") }})
                    then 'start'
                    when st_dwithin(g.posicao_veiculo_geo, end_pt, {{ var("buffer") }})
                    then 'end'
                    when st_dwithin(g.posicao_veiculo_geo, shape, {{ var("buffer") }})
                    then 'middle'
                    else 'out'
                end status_viagem
            from gps g
            inner join
                (
                    select *
                    from {{ ref("viagem_planejada") }}
                    where
                        {% if var("run_date") > var("DATA_SUBSIDIO_V6_INICIO") %}
                            data
                            = date_sub(date("{{ var(" run_date ") }}"), interval 1 day)
                        {% else %}
                            data between date_sub(
                                date("{{ var(" run_date ") }}"), interval 1 day
                            ) and date("{{ var(" run_date ") }}")
                        {% endif %}
                ) s
                on {% if var("run_date") > var("DATA_SUBSIDIO_V6_INICIO") %}
                    g.data_operacao = s.data
                {% else %} g.data = s.data
                {% endif %} and g.servico = s.servico
        )
    select *, '{{ var("version") }}' as versao_modelo
    from status_viagem

{% else %}

    -- 1. Seleciona sinais de GPS registrados no período
    with
        gps as (
            select
                g.* except (longitude, latitude, servico),
                servico,
                substr(id_veiculo, 2, 3) as id_empresa,
                st_geogpoint(longitude, latitude) posicao_veiculo_geo,
                date_sub(
                    date("{{ var(" run_date ") }}"), interval 1 day
                ) as data_operacao
            from
                -- `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo` g
                {{ ref("gps_sppo") }} g
            where
                (
                    data between date_sub(
                        date("{{ var(" run_date ") }}"), interval 1 day
                    ) and date("{{ var(" run_date ") }}")
                )
                -- Limita range de busca do gps de D-2 às 00h até D-1 às 3h
                and (
                    timestamp_gps
                    between datetime_sub(
                        datetime_trunc("{{ var(" run_date ") }}", day),
                        interval 1 day
                    ) and datetime_add(
                        datetime_trunc("{{ var(" run_date ") }}", day),
                        interval {{ gps_interval }} hour
                    )
                )
                and status != "Parado garagem"
        ),
        -- 2. Busca os shapes em formato geográfico
        shapes as (
            select *
            from {{ ref("shapes_geom_gtfs") }}
            -- rj-smtr.gtfs.shapes_geom
            where feed_start_date in ('{{ feed_start_dates|join("', '") }}')
        ),
        -- 3. Deduplica viagens planejadas
        viagem_planejada as (
            select distinct
                * except (
                    faixa_horaria_inicio,
                    faixa_horaria_fim,
                    partidas_total_planejada,
                    distancia_total_planejada,
                    shape,
                    start_pt,
                    end_pt
                )
            from {{ ref("viagem_planejada") }}
            where data = date_sub(date("{{ var(" run_date ") }}"), interval 1 day)
        ),
        deduplica_viagem_planejada as (
            select v.*, s.shape, s.start_pt, s.end_pt
            from viagem_planejada as v
            left join shapes as s using (feed_version, feed_start_date, shape_id)
        ),
        -- 4. Classifica a posição do veículo em todos os shapes possíveis de
        -- serviços de uma mesma empresa
        status_viagem as (
            select
                data_operacao as data,
                g.id_veiculo,
                g.id_empresa,
                g.timestamp_gps,
                timestamp_trunc(g.timestamp_gps, minute) as timestamp_minuto_gps,
                g.posicao_veiculo_geo,
                trim(g.servico, " ") as servico_informado,
                s.servico as servico_realizado,
                s.shape_id,
                s.sentido_shape,
                s.shape_id_planejado,
                s.trip_id,
                s.trip_id_planejado,
                s.sentido,
                s.start_pt,
                s.end_pt,
                s.distancia_planejada,
                ifnull(g.distancia, 0) as distancia,
                case
                    when
                        st_dwithin(g.posicao_veiculo_geo, start_pt, {{ var("buffer") }})
                    then 'start'
                    when st_dwithin(g.posicao_veiculo_geo, end_pt, {{ var("buffer") }})
                    then 'end'
                    when st_dwithin(g.posicao_veiculo_geo, shape, {{ var("buffer") }})
                    then 'middle'
                    else 'out'
                end status_viagem
            from gps g
            inner join
                (select * from deduplica_viagem_planejada) s
                on g.data_operacao = s.data
                and g.servico = s.servico
        )
    select *, '{{ var("version") }}' as versao_modelo
    from status_viagem
{% endif %}
