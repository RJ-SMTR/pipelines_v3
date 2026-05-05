{{
    config(
        materialized="view",
        alias="servico_contrato_abreviado",
    )
}}

{% set meses = {
    "janeiro": "Jan",
    "fevereiro": "Feb",
    "março": "Mar",
    "abril": "Apr",
    "maio": "May",
    "junho": "Jun",
    "julho": "Jul",
    "agosto": "Aug",
    "setembro": "Sep",
    "outubro": "Oct",
    "novembro": "Nov",
    "dezembro": "Dec",
} %}

select
    safe_cast(`Confirmação CGR` as bool) as confirmacao_cgr,
    safe_cast(`Serviço` as string) as servico,
    safe_cast(`Empresa` as string) as empresa,
    safe_cast(`Fase` as string) as fase,
    parse_date(
        '%b/%y',
        {% for pt, en in meses.items() %} replace({% endfor %}`Data`
        {% for pt, en in meses.items() %}, '{{ pt }}', '{{ en }}') {% endfor %}
    ) as data
from {{ source("subsidio_staging", "servico_contrato_abreviado") }}
where `Serviço` != ""
