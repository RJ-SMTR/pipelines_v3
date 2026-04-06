{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% set incremental_filter %}
    date(data) between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
    and timestamp_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

{% set transacao_retificada_staging = ref("staging_transacao_retificada") %}

{% if execute and is_incremental() %}
    {% set transacao_retificada_partitions_query %}
        select distinct concat("'", date(data_transacao), "'") as data_transacao
        from {{ transacao_retificada_staging }}
        where {{ incremental_filter }}
    {% endset %}

    {% set transacao_retificada_partitions = (
        run_query(transacao_retificada_partitions_query).columns[0].values()
    ) %}
{% endif %}

with
    transacao_retificada_staging as (
        select *
        from {{ transacao_retificada_staging }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    tipo_transacao as (
        select chave as id_tipo_transacao, valor as tipo_transacao,
        from {{ ref("dicionario_bilhetagem") }}
        where id_tabela = "transacao" and coluna = "id_tipo_transacao"
    ),
    dados_novos as (
        select
            id as id_retificacao,
            extract(date from data_transacao) as data,
            id_transacao,
            tto.tipo_transacao as tipo_transacao_jae_original,
            valor_transacao_original,
            ttr.tipo_transacao as tipo_transacao_jae_retificada,
            valor_transacao_retificada,
            data_retificacao as datetime_retificacao
        from {{ ref("staging_transacao_retificada") }} tr
        left join
            tipo_transacao tto on tr.tipo_transacao_original = tto.id_tipo_transacao
        left join
            tipo_transacao ttr on tr.tipo_transacao_retificada = ttr.id_tipo_transacao
    ),
    {% if is_incremental() and transacao_retificada_partitions | length > 0 %}
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
        dados_atuais as (
            select *, {{ sha_column }} as sha_dados_atuais from {{ this }}
        ),
        dados_completos as (
            select *, 0 as priority
            from dados_novos

            union all

            select
                * except (
                    versao,
                    datetime_ultima_atualizacao,
                    id_execucao_dbt,
                    sha_dados_atuais
                ),
                1 as priority
            from dados_atuais
        ),
        dados_completos_deduplicados as (
            select * except (priority), {{ sha_column }} as sha_dados_novos
            from dados_completos
            qualify
                row_number() over (partition by id_retificacao order by priority) = 1
        ),
        dados_completos_colunas_controle as (
            select
                n.* except (sha_dados_novos),
                '{{ var("version") }}' as versao,
                case
                    when
                        a.sha_dados_atuais is null
                        or n.sha_dados_novos != a.sha_dados_atuais
                    then current_datetime("America/Sao_Paulo")
                    else a.datetime_ultima_atualizacao
                end as datetime_ultima_atualizacao,
                case
                    when
                        a.sha_dados_atuais is null
                        or n.sha_dados_novos != a.sha_dados_atuais
                    then '{{ invocation_id }}'
                    else a.id_execucao_dbt
                end as id_execucao_dbt,
            from dados_completos_deduplicados n
            left join dados_atuais a using (id_retificacao)
        )
    {% else %}
        dados_completos_colunas_controle as (
            select
                *,
                '{{ var("version") }}' as versao,
                current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
                '{{ invocation_id }}' as id_execucao_dbt
            from dados_novos
        )
    {% endif %}
select *
from dados_completos_colunas_controle
