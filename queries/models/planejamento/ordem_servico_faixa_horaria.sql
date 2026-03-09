{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% if execute %}
    {% if is_incremental() %}
        {% set last_feed_version = get_last_feed_start_date(var("data_versao_gtfs")) %}
    {% endif %}
    {% if var("data_versao_gtfs") < var("DATA_SUBSIDIO_V11_INICIO") %}
        {% set intervalos = [
            {"inicio": "00", "fim": "03"},
            {"inicio": "03", "fim": "12"},
            {"inicio": "12", "fim": "21"},
            {"inicio": "21", "fim": "24"},
            {"inicio": "24", "fim": "03"},
        ] %}
    {% else %}
        {% set intervalos = [
            {"inicio": "00", "fim": "03"},
            {"inicio": "03", "fim": "06"},
            {"inicio": "06", "fim": "09"},
            {"inicio": "09", "fim": "12"},
            {"inicio": "12", "fim": "15"},
            {"inicio": "15", "fim": "18"},
            {"inicio": "18", "fim": "21"},
            {"inicio": "21", "fim": "24"},
            {"inicio": "24", "fim": "03"},
        ] %}
    {% endif %}
    {% set dias = ["dias_uteis", "sabado", "domingo", "ponto_facultativo"] %}
{% endif %}


with
    dados as (
        select
            safe_cast(data_versao as date) as data_versao,
            safe_cast(tipo_os as string) as tipo_os,
            safe_cast(servico as string) as servico,
            safe_cast(json_value(content, '$.vista') as string) as vista,
            safe_cast(json_value(content, '$.consorcio') as string) as consorcio,
            safe_cast(json_value(content, '$.extensao_ida') as float64) as extensao_ida,
            safe_cast(
                json_value(content, '$.extensao_volta') as float64
            ) as extensao_volta,
            {% for dia in dias %}
                safe_cast(
                    json_value(content, "$.horario_inicio_{{ dia|lower }}") as string
                ) as {{ "horario_inicio_" ~ dia | lower }},
                safe_cast(
                    json_value(content, "$.horario_fim_{{ dia|lower }}") as string
                ) as {{ "horario_fim_" ~ dia | lower }},
                safe_cast(
                    json_value(content, "$.partidas_ida_{{ dia|lower }}") as string
                ) as {{ "partidas_ida_" ~ dia | lower }},
                safe_cast(
                    json_value(content, "$.partidas_volta_{{ dia|lower }}") as string
                ) as {{ "partidas_volta_" ~ dia | lower }},
                safe_cast(
                    json_value(content, "$.viagens_{{ dia|lower }}") as string
                ) as {{ "viagens_" ~ dia | lower }},
                safe_cast(
                    json_value(content, "$.quilometragem_{{ dia|lower }}") as string
                ) as {{ "quilometragem_" ~ dia | lower }},
                {% for intervalo in intervalos %}
                    {% if intervalo.inicio != "24" %}
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_{{ dia|lower }}"
                            ) as string
                        )
                        as {{ "partidas_ida_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_" ~ dia | lower }},
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_{{ dia|lower }}"
                            ) as string
                        )
                        as {{ "partidas_volta_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_" ~ dia | lower }},
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_{{ dia|lower }}"
                            ) as string
                        )
                        as {{ "partidas_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_" ~ dia | lower }},
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_{{ dia|lower }}"
                            ) as string
                        )
                        as {{ "quilometragem_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_" ~ dia | lower }},
                    {% else %}
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_ida_entre_{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_dia_seguinte_{{ dia|lower }}"
                            ) as string
                        )
                        as {{ "partidas_ida_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_dia_seguinte_" ~ dia | lower }},
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_volta_entre_{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_dia_seguinte_{{ dia|lower }}"
                            ) as string
                        )
                        as {{ "partidas_volta_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_dia_seguinte_" ~ dia | lower }},
                        safe_cast(
                            json_value(
                                content,
                                "$.partidas_entre_{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_dia_seguinte_{{ dia|lower }}"
                            ) as string
                        )
                        as {{ "partidas_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_dia_seguinte_" ~ dia | lower }},
                        safe_cast(
                            json_value(
                                content,
                                "$.quilometragem_entre_{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_dia_seguinte_{{ dia|lower }}"
                            ) as string
                        )
                        as {{ "quilometragem_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_dia_seguinte_" ~ dia | lower }},
                    {% endif %}
                {% endfor %}
            {% endfor %}
        from
            {{
                source(
                    "br_rj_riodejaneiro_gtfs_staging", "ordem_servico_faixa_horaria"
                )
            }}
        {% if is_incremental() -%}
            where data_versao = '{{ var("data_versao_gtfs") }}'
        {% endif %}
    ),
    dados_dia as (
        select
            data_versao,
            tipo_os,
            servico,
            vista,
            consorcio,
            extensao_ida,
            extensao_volta,
            case
                when column_name like '%dias_uteis%'
                then 'Dia Útil'
                when column_name like '%sabado%'
                then 'Sabado'
                when column_name like '%domingo%'
                then 'Domingo'
                when column_name like '%ponto_facultativo%'
                then 'Ponto Facultativo'
            end as tipo_dia,
            max(
                case
                    when column_name like '%horario_inicio%'
                    then nullif(safe_cast(value as string), "—")
                end
            ) as horario_inicio,
            max(
                case
                    when column_name like '%horario_fim%'
                    then nullif(safe_cast(value as string), "—")
                end
            ) as horario_fim,
            max(
                case
                    when
                        column_name like '%partidas_ida_%'
                        and not column_name like '%entre%'
                    then safe_cast(value as int64)
                end
            ) as partidas_ida_dia,
            max(
                case
                    when
                        column_name like '%partidas_volta_%'
                        and not column_name like '%entre%'
                    then safe_cast(value as int64)
                end
            ) as partidas_volta_dia,
            max(
                case
                    when column_name like '%viagens_%' then safe_cast(value as float64)
                end
            ) as viagens_dia,
            max(
                case
                    when column_name like '%quilometragem_%'
                    then safe_cast(value as float64)
                end
            ) as quilometragem_dia
        from
            dados unpivot include nulls(
                value for column_name in (
                    {% for dia in dias %}
                        horario_inicio_{{ dia | lower }},
                        horario_fim_{{ dia | lower }},
                        partidas_ida_{{ dia | lower }},
                        partidas_volta_{{ dia | lower }},
                        viagens_{{ dia | lower }},
                        quilometragem_{{ dia | lower }}
                        {% if not loop.last %},{% endif %}
                    {% endfor %}
                )
            )
        group by 1, 2, 3, 4, 5, 6, 7, 8
    ),
    dados_faixa as (
        select
            data_versao,
            tipo_os,
            servico,
            vista,
            consorcio,
            extensao_ida,
            extensao_volta,
            case
                when column_name like '%dias_uteis%'
                then 'Dia Útil'
                when column_name like '%sabado%'
                then 'Sabado'
                when column_name like '%domingo%'
                then 'Domingo'
                when column_name like '%ponto_facultativo%'
                then 'Ponto Facultativo'
            end as tipo_dia,
            case
                {% for intervalo in intervalos %}
                    {% if intervalo.inicio != "24" %}
                        when
                            column_name
                            like '%{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h%'
                        then '{{ intervalo.inicio }}:00:00'
                    {% else %}
                        when
                            column_name
                            like '%{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_dia_seguinte%'
                        then '{{ intervalo.inicio }}:00:00'
                    {% endif %}
                {% endfor %}
            end as faixa_horaria_inicio,
            case
                {% for intervalo in intervalos %}
                    {% if intervalo.inicio != "24" %}
                        when
                            column_name
                            like '%{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h%'
                        then '{{ "%02d"|format(intervalo.fim|int - 1) }}:59:59'
                    {% else %}
                        when
                            column_name
                            like '%{{ intervalo.inicio }}h_e_{{ intervalo.fim }}h_dia_seguinte%'
                        then '26:59:59'
                    {% endif %}
                {% endfor %}
            end as faixa_horaria_fim,
            sum(
                case
                    when column_name like '%partidas_ida_entre%'
                    then safe_cast(value as int64)
                    else 0
                end
            ) as partidas_ida,
            sum(
                case
                    when column_name like '%partidas_volta_entre%'
                    then safe_cast(value as int64)
                    else 0
                end
            ) as partidas_volta,
            sum(
                case
                    when column_name like '%partidas_entre%'
                    then safe_cast(value as int64)
                    else 0
                end
            ) as partidas,
            sum(
                case
                    when column_name like '%quilometragem%'
                    then safe_cast(value as float64)
                    else 0
                end
            ) as quilometragem
        from
            dados unpivot include nulls(
                value for column_name in (
                    {% for dia in dias %}
                        {% for intervalo in intervalos %}
                            {% if intervalo.inicio != "24" %}
                                {{ "partidas_ida_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_" ~ dia | lower }},
                                {{ "partidas_volta_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_" ~ dia | lower }},
                                {{ "partidas_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_" ~ dia | lower }},
                                {{ "quilometragem_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_" ~ dia | lower }},
                            {% else %}
                                {{ "partidas_ida_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_dia_seguinte_" ~ dia | lower }},
                                {{ "partidas_volta_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_dia_seguinte_" ~ dia | lower }},
                                {{ "partidas_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_dia_seguinte_" ~ dia | lower }},
                                {{ "quilometragem_entre_" ~ intervalo.inicio ~ "h_e_" ~ intervalo.fim ~ "h_dia_seguinte_" ~ dia | lower }}
                            {% endif %}
                        {% endfor %}
                        {% if not loop.last %},{% endif %}
                    {% endfor %}
                )
            )
        where column_name like '%entre%'
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    ),
    dados_agrupados as (
        select
            dd.*,
            df.faixa_horaria_inicio,
            df.faixa_horaria_fim,
            df.partidas_ida,
            df.partidas_volta,
            case
                when
                    date('{{ var("data_versao_gtfs") }}')
                    < date('{{var("DATA_GTFS_V2_INICIO") }}')
                then df.partidas
                else df.partidas_ida + df.partidas_volta
            end as partidas,
            df.quilometragem
        from dados_dia as dd
        inner join
            dados_faixa as df
            on dd.data_versao = df.data_versao
            and dd.tipo_os = df.tipo_os
            and dd.servico = df.servico
            and dd.tipo_dia = df.tipo_dia
    )
select
    fi.feed_version,
    fi.feed_start_date,
    fi.feed_end_date,
    d.* except (data_versao),
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from dados_agrupados as d
left join {{ ref("feed_info_gtfs") }} as fi on d.data_versao = fi.feed_start_date
{% if is_incremental() -%}
    where
        d.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
{% else %} where d.data_versao >= '{{ var("DATA_SUBSIDIO_V9_INICIO") }}'
{% endif %} and d.data_versao < '{{ var("DATA_GTFS_V4_INICIO") }}'
