{{
    config(
        materialized="incremental",
        alias="perfil_funcionamento_historico",
        incremental_strategy="merge",
        unique_key="id_perfil_funcionamento_historico",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
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
        /*
        considera apenas exceções vigentes na data da captura: exceção
        expirada ou futura que permanece na lista da fonte não entra no
        histórico do dia
        */
        select
            data,
            perfil_funcionamento_codigo as id_perfil_funcionamento,
            area_codigo as id_area,
            perfil_funcionamento_excecao_datetime_inicio as datetime_inicio,
            perfil_funcionamento_excecao_datetime_fim as datetime_fim,
            perfil_funcionamento_excecao_motivo as motivo,
            perfil_funcionamento_excecao_decisao as decisao
        from {{ ref("staging_perfil_funcionamento_excecao_riorotativo") }}
        where
            (
                data >= date(perfil_funcionamento_excecao_datetime_inicio)
                or perfil_funcionamento_excecao_datetime_inicio is null
            )
            and (
                data <= date(perfil_funcionamento_excecao_datetime_fim)
                or perfil_funcionamento_excecao_datetime_fim is null
            )
            {% if is_incremental() %} and {{ incremental_filter }} {% endif %}
    ),
    dados_novos_chave as (
        select
            to_hex(
                sha256(
                    concat(
                        cast(data as string),
                        '-',
                        ifnull(id_perfil_funcionamento, 'n/a'),
                        '-',
                        ifnull(id_area, 'n/a'),
                        '-',
                        ifnull(decisao, 'n/a')
                    )
                )
            ) as id_perfil_funcionamento_historico,
            *
        from dados_novos
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo from dados_novos_chave
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}
            select
                id_perfil_funcionamento_historico,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }}
            where {{ incremental_filter }}
        {% else %}
            select
                cast(null as string) as id_perfil_funcionamento_historico,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_perfil_funcionamento_historico)
        from sha_dados_novos as n
        left join sha_dados_atuais as a using (id_perfil_funcionamento_historico)
    ),
    perfil_funcionamento_colunas_controle as (
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
from perfil_funcionamento_colunas_controle
