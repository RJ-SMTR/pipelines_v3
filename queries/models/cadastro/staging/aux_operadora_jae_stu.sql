{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
    )
}}

{% set columns = [
    "id_operadora",
    "modo",
    "modo_stu",
    "modo_jae",
    "operadora",
    "operadora_completo",
    "tipo_operadora",
    "tipo_documento",
    "documento",
    "id_operadora_stu",
    "id_operadora_jae",
    "id_processo",
    "data_processo",
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
    stu as (
        select *, timestamp_captura as datetime_inicio_validade
        from {{ ref("aux_operadora_stu") }}
    ),
    jae as (
        select *, timestamp_captura as datetime_inicio_validade
        from {{ ref("aux_operadora_jae") }}
    ),
    operadora as (
        select
            s.datetime_inicio_validade as datetime_inicio_validade_stu,
            j.datetime_inicio_validade as datetime_inicio_validade_jae,
            coalesce(s.perm_autor, j.cd_operadora_transporte) as id_operadora,
            upper(
                regexp_replace(
                    normalize(coalesce(s.nome_operadora, j.nm_cliente), nfd), r"\pM", ''
                )
            ) as operadora_completo,
            s.tipo_permissao as tipo_operadora,
            coalesce(j.modo, s.modo) as modo,
            s.modo_stu,
            j.modo_jae,
            s.processo as id_processo,
            s.data_registro as data_processo,
            coalesce(s.documento, j.documento) as documento,
            coalesce(s.tipo_documento, j.tipo_documento) as tipo_documento,
            s.perm_autor as id_operadora_stu,
            j.cd_operadora_transporte as id_operadora_jae,
            safe_cast(
                j.in_situacao_atividade as boolean
            ) as indicador_operador_ativo_jae,
        from {{ ref("aux_operadora_stu") }} as s
        full outer join
            {{ ref("aux_operadora_jae") }} as j
            on s.documento = j.nr_documento
            and s.modo = j.modo_join
    )
select
    id_operadora,
    modo,
    modo_stu,
    modo_jae,
    case
        when tipo_documento = "CNPJ"
        then operadora_completo
        else regexp_replace(operadora_completo, '[^ ]', '*')
    end as operadora,
    operadora_completo,
    tipo_operadora,
    tipo_documento,
    documento,
    id_operadora_stu,
    id_operadora_jae,
    id_processo,
    data_processo,
    indicador_operador_ativo_jae
from operadora
where modo not in ("Escolar", "Táxi")
