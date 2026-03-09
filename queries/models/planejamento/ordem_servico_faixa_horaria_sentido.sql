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
    {% set intervalos = [
        {"inicio": "00", "fim": "01"},
        {"inicio": "01", "fim": "02"},
        {"inicio": "02", "fim": "03"},
        {"inicio": "03", "fim": "04"},
        {"inicio": "04", "fim": "05"},
        {"inicio": "05", "fim": "06"},
        {"inicio": "06", "fim": "09"},
        {"inicio": "09", "fim": "12"},
        {"inicio": "12", "fim": "15"},
        {"inicio": "15", "fim": "18"},
        {"inicio": "18", "fim": "21"},
        {"inicio": "21", "fim": "22"},
        {"inicio": "22", "fim": "23"},
        {"inicio": "23", "fim": "24"},
    ] %}
    {% set dias = ["dias_uteis", "sabado", "domingo", "ponto_facultativo"] %}
{% endif %}

with
    dados as (
        select
            safe_cast(data_versao as date) as data_versao,
            safe_cast(tipo_os as string) as tipo_os,
            safe_cast(servico as string) as servico,
            safe_cast(sentido as string) as sentido,
            safe_cast(json_value(content, '$.vista') as string) as vista,
            safe_cast(json_value(content, '$.consorcio') as string) as consorcio,
            safe_cast(json_value(content, '$.extensao') as float64) as extensao,
            {% for dia in dias %}
                {% for intervalo in intervalos %}
                    safe_cast(
                        json_value(
                            content,
                            "$.partidas_entre_{{ intervalo.inicio }}h_a_{{ intervalo.fim }}h_{{ dia|lower }}"
                        ) as string
                    )
                    as {{ "partidas_entre_" ~ intervalo.inicio ~ "h_a_" ~ intervalo.fim ~ "h_" ~ dia | lower }},
                    safe_cast(
                        json_value(
                            content,
                            "$.quilometragem_entre_{{ intervalo.inicio }}h_a_{{ intervalo.fim }}h_{{ dia|lower }}"
                        ) as string
                    )
                    as {{ "quilometragem_entre_" ~ intervalo.inicio ~ "h_a_" ~ intervalo.fim ~ "h_" ~ dia | lower }},
                {% endfor %}
            {% endfor %}
        from
            {{
                source(
                    "br_rj_riodejaneiro_gtfs_staging",
                    "ordem_servico_faixa_horaria_sentido",
                )
            }}
        {% if is_incremental() -%}
            where data_versao = '{{ var("data_versao_gtfs") }}'
        {% endif %}
    ),
    dados_faixa as (
        select
            data_versao,
            tipo_os,
            servico,
            vista,
            consorcio,
            sentido,
            extensao,
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
                    when
                        column_name
                        like '%{{ intervalo.inicio }}h_a_{{ intervalo.fim }}h%'
                    then '{{ intervalo.inicio }}:00:00'
                {% endfor %}
            end as faixa_horaria_inicio,
            case
                {% for intervalo in intervalos %}
                    when
                        column_name
                        like '%{{ intervalo.inicio }}h_a_{{ intervalo.fim }}h%'
                    then '{{ "%02d"|format(intervalo.fim|int - 1) }}:59:59'
                {% endfor %}
            end as faixa_horaria_fim,
            sum(
                case
                    when column_name like '%partidas%'
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
                            {{ "partidas_entre_" ~ intervalo.inicio ~ "h_a_" ~ intervalo.fim ~ "h_" ~ dia | lower }},
                            {{ "quilometragem_entre_" ~ intervalo.inicio ~ "h_a_" ~ intervalo.fim ~ "h_" ~ dia | lower }}
                            {% if intervalo.inicio != "23" %},{% endif %}
                        {% endfor %}
                        {% if not loop.last %},{% endif %}
                    {% endfor %}
                )
            )
        where column_name like '%entre%'
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    )
select
    fi.feed_version,
    fi.feed_start_date,
    fi.feed_end_date,
    d.* except (data_versao),
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from dados_faixa as d
left join {{ ref("feed_info_gtfs") }} as fi on d.data_versao = fi.feed_start_date
-- left join `rj-smtr.gtfs.feed_info` as fi on d.data_versao = fi.feed_start_date
where
    {% if is_incremental() -%}
        d.data_versao in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and fi.feed_start_date
        in ('{{ last_feed_version }}', '{{ var("data_versao_gtfs") }}')
        and
    {% endif %} d.data_versao
    >= '{{ var("DATA_GTFS_V4_INICIO") }}'
