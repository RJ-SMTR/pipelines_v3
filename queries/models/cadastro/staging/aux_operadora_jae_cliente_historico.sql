{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set columns = [
    "id_cliente",
    "tipo_documento",
    "documento",
    "nome",
] %}

{% set sha_column %}
    sha256(
        concat(
            {% for c in columns %}
                ifnull(cast({{ c }} as string), 'n/a')

                {% if not loop.last %}, {% endif %}

            {% endfor %}
        )
    )
{% endset %}

with
    dados_novos as (
        select
            date(timestamp_captura) as data_particao,
            timestamp_captura as datetime_inicio_validade,
            cd_cliente as id_cliente,
            case
                when in_tipo_pessoa_fisica_juridica = "F"
                then "CPF"
                when in_tipo_pessoa_fisica_juridica = "J"
                then "CNPJ"
            end as tipo_documento,
            nr_documento as documento,
            nm_cliente as nome
        from {{ ref("staging_cliente") }}
        where
            cd_cliente
            in (select cd_cliente from {{ ref("staging_operadora_transporte") }})
            {% if is_incremental() %}
                and date(data) between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
                and timestamp_captura
                between datetime("{{var('date_range_start')}}") and datetime(
                    "{{var('date_range_end')}}"
                )

            {% endif %}
    ),
    dados_completos as (
        select *, 0 as priority
        from dados_novos
        {% if is_incremental() %}
            union all by name

            select * except (datetime_fim_validade), 1 as priority
            from {{ this }}
        {% endif %}
    ),
    mudanca_valor as (
        select
            * except (priority),
            {{ sha_column }} != ifnull(
                lag({{ sha_column }}) over (
                    partition by id_cliente order by datetime_inicio_validade, priority
                ),
                cast("" as bytes)
            ) as indicador_mudanca_valor
        from dados_completos
    ),
    cliente_datetime_fim_validade as (
        select
            data_particao,
            datetime_inicio_validade,
            lead(datetime_inicio_validade) over (
                partition by id_cliente order by datetime_inicio_validade
            ) as datetime_fim_validade,
            {{ columns | join(", ") }}
        from mudanca_valor
        where indicador_mudanca_valor
    )
select *
from cliente_datetime_fim_validade
