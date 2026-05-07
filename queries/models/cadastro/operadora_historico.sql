{{ config(materialized="table", tags=["identificacao"]) }}


{% if table_exists(this) %}

    {% set columns = (
        list_columns()
        | reject(
            "in",
            ["versao", "datetime_ultima_atualizacao", "id_execucao_dbt"],
        )
        | list
    ) %}

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

{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}

{% endif %}

with
    operadora_tratado as (
        select
            *,
            upper(
                regexp_replace(normalize(operadora_jae, nfd), r"\pM", '')
            ) as operadora_tratado_jae,
            upper(
                regexp_replace(normalize(operadora_stu, nfd), r"\pM", '')
            ) as operadora_tratado_stu
        from {{ ref("aux_operadora_jae_stu_historico") }}
    ),
    operadora_jae_stu_cnpj as (
        select
            ifnull(
                greatest(js.datetime_inicio_validade, c.datetime_inicio_validade),
                js.datetime_inicio_validade
            ) as datetime_inicio_validade,
            js.id_operadora_jae,
            js.id_operadora_stu,
            js.id_cliente,
            js.modo_jae,
            js.modo_stu,
            js.tipo_documento,
            js.documento,
            case
                when js.tipo_documento = "CNPJ"
                then js.operadora_tratado_jae
                else regexp_replace(js.operadora_tratado_jae, '[^ ]', '*')
            end as operadora_jae,
            case
                when js.tipo_documento = "CNPJ"
                then js.operadora_tratado_stu
                else regexp_replace(js.operadora_tratado_stu, '[^ ]', '*')
            end as operadora_stu,
            js.operadora_tratado_jae as operadora_completo_jae,
            js.operadora_tratado_stu as operadora_completo_stu,
            c.razao_social,
            c.nome_fantasia,
            c.situacao_cadastral,
            c.situacao_especial,
            js.indicador_operador_ativo_jae
        from operadora_tratado js
        left join
            {{ ref("aux_operadora_cnpj") }} c
            on js.documento = c.cnpj
            and js.tipo_documento = 'CNPJ'
            and (
                (
                    c.datetime_inicio_validade >= js.datetime_inicio_validade
                    and (
                        c.datetime_inicio_validade < js.datetime_fim_validade
                        or js.datetime_fim_validade is null
                    )
                )
                or (
                    c.datetime_fim_validade is not null
                    and c.datetime_fim_validade >= js.datetime_inicio_validade
                    and (
                        c.datetime_fim_validade < js.datetime_fim_validade
                        or js.datetime_fim_validade is null
                    )
                )
                or (
                    c.datetime_inicio_validade < js.datetime_inicio_validade
                    and (
                        c.datetime_fim_validade > js.datetime_inicio_validade
                        or c.datetime_fim_validade is null
                    )

                )
            )
    ),
    fim_validade as (
        select
            datetime_inicio_validade,
            lead(datetime_inicio_validade) over (
                partition by id_operadora_jae order by datetime_inicio_validade
            ) as datetime_fim_validade,
            * except (datetime_inicio_validade)
        from operadora_jae_stu_cnpj
    ),
    sha_dados_novos as (select *, {{ sha_column }} as sha_dado_novo from fim_validade),
    sha_dados_atuais as (
        {% if table_exists(this) %}

            select
                datetime_inicio_validade,
                id_operadora_jae,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }}

        {% else %}
            select
                cast(null as datetime) as datetime_inicio_validade,
                cast(null as string) as id_operadora_jae,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_operadora_jae, datetime_inicio_validade)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_operadora_jae, datetime_inicio_validade)
    ),
    operadora_colunas_controle as (
        select
            * except (
                sha_dado_novo,
                sha_dado_atual,
                datetime_ultima_atualizacao_atual,
                id_execucao_dbt_atual
            ),
            '{{ var("version") }}' as versao,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then current_datetime("America/Sao_Paulo")
                else datetime_ultima_atualizacao_atual
            end as datetime_ultima_atualizacao,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then '{{ invocation_id }}'
                else id_execucao_dbt_atual
            end as id_execucao_dbt
        from sha_dados_completos
    )
select *
from operadora_colunas_controle
