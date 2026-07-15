{{ config(alias="guardador_veiculo") }}

{% set entidades = [
    {"cnpj": "05019730000158", "source": "entidade_05019730000158"},
    {"cnpj": "34152025000122", "source": "entidade_34152025000122"},
] %}


with
    dados as (
        {% for entidade in entidades %}
            select
                data,
                lpad(safe_cast(cpf as string), 11, '0') as documento,
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
    row_number() over (
        partition by data, cnpj, documento order by datetime_captura desc
    )
    = 1
