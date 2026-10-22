{{
    config(
        materialized="table",
    )
}}

{% set columns = [
    "id_operadora_jae",
    "id_cliente",
    "id_modo",
    "modo",
    "modo_join",
    "indicador_operador_ativo_jae",
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
            ot.timestamp_captura as datetime_inicio_validade,
            ot.cd_operadora_transporte as id_operadora_jae,
            ot.cd_cliente as id_cliente,
            ot.cd_tipo_modal as id_modo,
            m.modo,
            case when ot.cd_tipo_modal = '3' then 'Ônibus' else m.modo end as modo_join,
            ot.in_situacao_atividade = '1' as indicador_operador_ativo_jae
        from {{ ref("staging_operadora_transporte") }} as ot
        join {{ ref("modos") }} m on ot.cd_tipo_modal = m.id_modo and m.fonte = "jae"
    ),
    mudanca_valor as (
        select
            *,
            {{ sha_column }} != ifnull(
                lag({{ sha_column }}) over (
                    partition by id_operadora_jae order by datetime_inicio_validade
                ),
                cast("" as bytes)
            ) as indicador_mudanca_valor
        from dados_novos
    ),
    operadora_datetime_fim_validade as (
        select
            datetime_inicio_validade,
            lead(datetime_inicio_validade) over (
                partition by id_operadora_jae order by datetime_inicio_validade
            ) as datetime_fim_validade,
            {{ columns | join(", ") }}
        from mudanca_valor
        where indicador_mudanca_valor
    )
select *
from operadora_datetime_fim_validade
