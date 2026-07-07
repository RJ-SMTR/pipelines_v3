{{ config(alias="riorotativo_credenciado") }}

{% set entidades = [
    {"cnpj": "42498733000148", "source": "entidade_42498733000148"},
] %}


with
    dados as (
        {% for entidade in entidades %}
            select
                data,
                safe_cast(cpf as string) as documento,
                "CPF" as tipo_documento,
                "{{ entidade.cnpj }}" as cnpj,
                datetime(
                    parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
                    "America/Sao_Paulo"
                ) as datetime_captura
            from {{ source("source_riorotativo", entidade.source) }}

            {% if not loop.last %}
                union all
            {% endif %}

        {% endfor %}
    )
select *
from dados
qualify
    row_number() over (partition by cnpj, documento order by datetime_captura desc) = 1
