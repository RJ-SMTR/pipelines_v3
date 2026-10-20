{{ config(alias="ordem_servico_faixa_horaria_sentido") }}

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

select
    safe_cast(data_versao as date) as data_versao,
    safe_cast(servico as string) as servico,
    safe_cast(json_value(content, '$.vista') as string) as vista,
    safe_cast(json_value(content, '$.consorcio') as string) as consorcio,
    safe_cast(sentido as string) as sentido,
    safe_cast(json_value(content, '$.extensao') as string) as extensao,
    {% for dia in dias %}
        {% for intervalo in intervalos %}
            safe_cast(
                json_value(
                    content,
                    "$.partidas_entre_{{ intervalo.inicio }}h_a_{{ intervalo.fim }}h_{{ dia }}"
                ) as string
            )
            as {{
                (
                    "partidas_entre_"
                    ~ intervalo.inicio
                    ~ "h_a_"
                    ~ intervalo.fim
                    ~ "h_"
                    ~ dia
                )
            }},
            safe_cast(
                json_value(
                    content,
                    "$.quilometragem_entre_{{ intervalo.inicio }}h_a_{{ intervalo.fim }}h_{{ dia }}"
                ) as string
            )
            as {{
                (
                    "quilometragem_entre_"
                    ~ intervalo.inicio
                    ~ "h_a_"
                    ~ intervalo.fim
                    ~ "h_"
                    ~ dia
                )
            }},
        {% endfor %}
    {% endfor %}
    safe_cast(tipo_os as string) as tipo_os,
    timestamp_captura
from {{ source("source_gtfs", "ordem_servico_faixa_horaria_sentido") }}
