-- depends_on: {{ ref('subsidio_data_versao_efetiva') }}
{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["id_viagem"],
        incremental_strategy="insert_overwrite",
        labels={"dashboard": "yes"},
    )
}}

{% if execute %}
    {% set result = run_query(
        "SELECT coalesce(data_versao_shapes, feed_start_date) FROM "
        ~ ref("subsidio_data_versao_efetiva")
        ~ " WHERE data = DATE_SUB(DATE('"
        ~ var("run_date")
        ~ "'), INTERVAL 1 DAY)"
    ) %}
    {% set feed_start_date = result.columns[0].values()[0] %}
{% endif %}
-- 1. Identifica viagens que estão dentro do quadro planejado (por
-- enquanto, consideramos o dia todo).
with
    viagem_periodo as (
        select distinct
            p.consorcio,
            p.vista,
            p.tipo_dia,
            v.*,
            p.inicio_periodo,
            p.fim_periodo,
            p.id_tipo_trajeto,
            0 as tempo_planejado,
        from
            (
                select distinct
                    consorcio,
                    vista,
                    data,
                    tipo_dia,
                    trip_id_planejado as trip_id,
                    servico,
                    inicio_periodo,
                    fim_periodo,
                    id_tipo_trajeto
                from {{ ref("viagem_planejada") }}
                {% if is_incremental() %}
                    where
                        data = date_sub(date("{{ var(" run_date ") }}"), interval 1 day)
                {% endif %}
            ) p
        inner join
            (
                select distinct *
                from {{ ref("viagem_conformidade") }}
                {% if is_incremental() %}
                    where
                        data = date_sub(date("{{ var(" run_date ") }}"), interval 1 day)
                {% endif %}
            ) v
            on v.trip_id = p.trip_id
            and v.data = p.data
    ),
    shapes as (
        select *
        from {{ ref("shapes_geom_gtfs") }}
        where feed_start_date = "{{ feed_start_date }}"
    ),
    -- 2. Seleciona viagens completas de acordo com a conformidade
    viagem_comp_conf as (
        select distinct
            consorcio,
            data,
            tipo_dia,
            id_empresa,
            id_veiculo,
            id_viagem,
            servico_informado,
            servico_realizado,
            vista,
            trip_id,
            shape_id,
            sentido,
            datetime_partida,
            datetime_chegada,
            inicio_periodo,
            fim_periodo,
            case
                when servico_realizado = servico_informado
                then "Completa linha correta"
                else "Completa linha incorreta"
            end as tipo_viagem,
            tempo_viagem,
            tempo_planejado,
            distancia_planejada,
            distancia_aferida,
            n_registros_shape,
            n_registros_total,
            n_registros_minuto,
            velocidade_media,
            perc_conformidade_shape,
            perc_conformidade_distancia,
            perc_conformidade_registros,
            0 as perc_conformidade_tempo,
            -- round(100 * tempo_viagem/tempo_planejado, 2) as perc_conformidade_tempo,
            id_tipo_trajeto,
            '{{ var("version") }}' as versao_modelo,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from viagem_periodo v
        left join shapes as s using (shape_id)
        where
            (
            {% if var("run_date") > var("DATA_SUBSIDIO_V12_INICIO") %}
                    velocidade_media <= {{ var("conformidade_velocidade_min") }}
                    or (
                        (
                            (
                                st_numgeometries(
                                    st_intersection(
                                        st_buffer(start_pt, {{ var("buffer") }}), shape
                                    )
                                )
                                > 1
                                or st_numgeometries(
                                    st_intersection(
                                        st_buffer(end_pt, {{ var("buffer") }}), shape
                                    )
                                )
                                > 1
                            )
                            and st_distance(start_pt, end_pt)
                            < {{
                                var(
                                    "distancia_inicio_fim_conformidade_velocidade_min"
                                )
                            }}
                        )
                        and sentido != "C"
                    )
                ) and (
            {% endif %}
                perc_conformidade_shape >= {{ var("perc_conformidade_shape_min") }}
            )
            and (
                perc_conformidade_distancia
                >= {{ var("perc_conformidade_distancia_min") }}
            )
            and (
                perc_conformidade_registros
                >= {{ var("perc_conformidade_registros_min") }}
            )
            {% if var("run_date") == "2023-01-01" %}
                -- Reveillon (2022-12-31)
                and (
                    -- 1. Viagens pre fechamento das vias
                    (
                        fim_periodo = "22:00:00"
                        and datetime_chegada <= "2022-12-31 22:05:00"
                    )
                    or (
                        fim_periodo = "18:00:00"
                        and datetime_chegada <= "2022-12-31 18:05:00"
                    )  -- 18h as 5h
                    -- 2. Viagens durante fechamento das vias
                    or (
                        inicio_periodo = "22:00:00"
                        and datetime_partida >= "2022-12-31 21:55:00"
                    )  -- 22h as 5h/10h
                    or (
                        inicio_periodo = "18:00:00"
                        and datetime_partida >= "2022-12-31 17:55:00"
                    )  -- 18h as 5h
                    -- 3. Viagens que nao sao afetadas pelo fechamento das vias
                    or (inicio_periodo = "00:00:00" and fim_periodo = "23:59:59")
                )
            -- Feriado do Dia da Fraternidade Universal (2023-01-01)
            {% elif var("run_date") == "2023-01-02" %}
                and (
                    -- 1. Viagens durante fechamento das vias
                    (
                        fim_periodo = "05:00:00"
                        and datetime_partida <= "2023-01-01 05:05:00"
                    )
                    or (
                        fim_periodo = "10:00:00"
                        and datetime_partida <= "2023-01-01 10:05:00"
                    )
                    -- 2. Viagens pos abertura das vias
                    or (
                        inicio_periodo = "05:00:00"
                        and datetime_partida >= "2023-01-01 04:55:00"
                    )
                    or (
                        inicio_periodo = "10:00:00"
                        and datetime_partida >= "2023-01-01 09:55:00"
                    )
                    -- 3. Viagens que nao sao afetadas pelo fechamento das vias
                    or (inicio_periodo = "00:00:00" and fim_periodo = "23:59:59")
                )
            {% elif var("run_date") in ("2024-05-05", "2024-05-06") %}
                -- Apuração "Madonna · The Celebration Tour in Rio"
                and (datetime_partida between inicio_periodo and fim_periodo)
            {% endif %}
    ),
    -- 3. Filtra viagens com mesma chegada e partida pelo maior % de conformidade do
    -- shape
    filtro_desvio as (
        select
            {% if var("run_date") > var("DATA_SUBSIDIO_V6_INICIO") %}
                * except (rn, id_tipo_trajeto)
            {% else %} * except (rn)
            {% endif %}
        from
            (
                select
                    *,
                    {% if var("run_date") > var("DATA_SUBSIDIO_V7_INICIO") %}
                        -- Apuração "Madonna · The Celebration Tour in Rio"
                        row_number() over (
                            partition by id_veiculo, datetime_partida, datetime_chegada
                            order by
                                perc_conformidade_shape desc,
                                id_tipo_trajeto,
                                distancia_planejada desc
                        ) as rn
                    {% elif var("run_date") > var("DATA_SUBSIDIO_V6_INICIO") %}
                        row_number() over (
                            partition by id_veiculo, datetime_partida, datetime_chegada
                            order by perc_conformidade_shape desc, id_tipo_trajeto
                        ) as rn
                    {% else %}
                        row_number() over (
                            partition by id_veiculo, datetime_partida, datetime_chegada
                            order by perc_conformidade_shape desc
                        ) as rn
                    {% endif %}
                from viagem_comp_conf
            )
        where rn = 1
    ),
    -- 4. Filtra viagens com partida ou chegada diferentes pela maior distancia
    -- percorrida
    filtro_partida as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by id_veiculo, datetime_partida
                        order by distancia_planejada desc
                    ) as rn
                from filtro_desvio
            )
        where rn = 1
    )
-- filtro_chegada
select * except (rn)
from
    (
        select
            *,
            row_number() over (
                partition by id_veiculo, datetime_chegada
                order by distancia_planejada desc
            ) as rn
        from filtro_partida
    )
where rn = 1
