{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key=["cnpj", "id_cliente"],
    )
}}

{% set staging_riorotativo_credenciado = ref("staging_riorotativo_credenciado") %}


{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
    and datetime_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

{% if execute and is_incremental() %}
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
    dados_novos as (
        select documento, tipo_documento, nome, id_cliente, cnpj
        from {{ staging_riorotativo_credenciado }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (
                partition by cnpj, id_cliente order by datetime_captura desc
            )
            = 1
    ),
    sha_dados_novos as (select *, {{ sha_column }} as sha_dado_novo from dados_novos),
    sha_dados_atuais as (
        {% if is_incremental() %}

            select
                cnpj,
                id_cliente,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }}

        {% else %}
            select
                cast(null as string) as cnpj,
                cast(null as string) as id_cliente,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (cnpj, id_cliente)
        from sha_dados_novos n
        left join sha_dados_atuais a using (cnpj, id_cliente)
    ),
    agente_credenciado_colunas_controle as (
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
from agente_credenciado_colunas_controle
