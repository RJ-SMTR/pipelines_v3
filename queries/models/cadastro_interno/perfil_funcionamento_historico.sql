{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        unique_key="id_perfil_funcionamento_historico",
    )
}}

{% set staging_riorotativo_perfil_funcionamento_excecao = ref(
    "staging_riorotativo_perfil_funcionamento_excecao"
) %}


{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
    and datetime_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}


with
    dados_novos as (
        select
            perfil_funcionamento_codigo as id_perfil_funcionamento,
            area_codigo as id_area,
            perfil_funcionamento_excecao_data_inicio as data_inicio,
            perfil_funcionamento_excecao_data_fim as data_fim,
            perfil_funcionamento_excecao_horario_inicio as horario_inicio,
            perfil_funcionamento_excecao_horario_fim as horario_fim,
            perfil_funcionamento_excecao_motivo as motivo,
            perfil_funcionamento_excecao_decisao as decisao
        from {{ staging_riorotativo_perfil_funcionamento_excecao }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    dados_novos_chave as (
        select
            to_hex(
                sha256(
                    concat(
                        ifnull(id_perfil_funcionamento, 'n/a'),
                        ifnull(id_area, 'n/a'),
                        ifnull(decisao, 'n/a'),
                        ifnull(cast(data_inicio as string), 'n/a'),
                        ifnull(cast(data_fim as string), 'n/a'),
                        ifnull(horario_inicio, 'n/a'),
                        ifnull(horario_fim, 'n/a')
                    )
                )
            ) as id_perfil_funcionamento_historico,
            *
        from dados_novos
    ),
    dados_atuais as (
        {% if is_incremental() %}
            select
                id_perfil_funcionamento_historico,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }}
        {% else %}
            select
                cast(null as string) as id_perfil_funcionamento_historico,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    dados_completos as (
        select n.*, a.* except (id_perfil_funcionamento_historico)
        from dados_novos_chave n
        left join dados_atuais a using (id_perfil_funcionamento_historico)
    ),
    perfil_funcionamento_colunas_controle as (
        select
            * except (datetime_ultima_atualizacao_atual, id_execucao_dbt_atual),
            '{{ var("version") }}' as versao,
            coalesce(
                datetime_ultima_atualizacao_atual, current_datetime("America/Sao_Paulo")
            ) as datetime_ultima_atualizacao,
            coalesce(id_execucao_dbt_atual, '{{ invocation_id }}') as id_execucao_dbt
        from dados_completos
    )
select *
from perfil_funcionamento_colunas_controle
