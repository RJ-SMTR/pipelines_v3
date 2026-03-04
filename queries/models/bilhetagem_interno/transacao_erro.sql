{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set aux_transacao_erro_completa = ref("aux_transacao_erro_completa") %}
{% set transacao = ref("transacao") %}

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

    {% set partitions_query %}
        select concat("'", parse_date("%Y%m%d", partition_id), "'") as data
        from
            `{{ aux_transacao_erro_completa.database }}.{{ aux_transacao_erro_completa.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        where
            table_name = "{{ aux_transacao_erro_completa.identifier }}"
            and partition_id != "__NULL__"
            and datetime(
                last_modified_time,
                "America/Sao_Paulo"
            ) between datetime_add(datetime("{{var('date_range_start')}}"), interval 1 hour) and (
                datetime_add(datetime("{{var('date_range_end')}}"), interval 1 hour)
            )

        union distinct

        select concat("'", parse_date("%Y%m%d", partition_id), "'") as data
        from
            `{{ transacao.database }}.{{ transacao.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        where
            table_name = "{{ transacao.identifier }}"
            and partition_id != "__NULL__"
            and datetime(
                last_modified_time,
                "America/Sao_Paulo"
            ) between datetime_add(datetime("{{var('date_range_start')}}"), interval 1 hour) and (
                datetime_add(datetime("{{var('date_range_end')}}"), interval 1 hour)
            )

    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% else %}
    {% set sha_column %}
    cast(null as bytes)
    {% endset %}
{% endif %}

with
    transacao_erro_completa as (
        select *
        from {{ aux_transacao_erro_completa }}
        where
            {% if is_incremental() %}
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} data = "0001-01-01"
                {% endif %}
            {% else %} data >= "0001-01-01"
            {% endif %}
    ),
    transacao_erro_deduplicada as (
        select *
        from transacao_erro_completa
        qualify
            row_number() over (
                partition by hash_cartao, datetime_transacao, id_validador
                order by datetime_inclusao
            )
            = 1
    ),
    transacao as (
        select hash_cartao, datetime_transacao, id_validador
        from {{ ref("transacao") }}
        where
            {% if is_incremental() %}
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} data = "0001-01-01"
                {% endif %}
            {% else %} data >= "0001-01-01"
            {% endif %}
    ),
    transacao_erro_reprocessadas as (
        select distinct tr.id_transacao_recebida
        from transacao_erro_deduplicada tr
        join
            transacao t
            on tr.hash_cartao = t.hash_cartao
            and tr.datetime_transacao = t.datetime_transacao
            and tr.id_validador = t.id_validador
    ),
    dados_novos as (
        select *
        from transacao_erro_deduplicada
        where
            id_transacao_recebida
            not in (select id_transacao_recebida from transacao_erro_reprocessadas)
    ),
    {% if is_incremental() %}

        dados_atuais as (
            select *
            from {{ this }}
            where
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} data = "0001-01-01"
                {% endif %}

        ),
    {% endif %}
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo
        from dados_novos
        qualify
            row_number() over (
                partition by id_transacao_recebida
                order by priority, datetime_captura desc
            )
            = 1
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}

            select
                id_transacao_recebida,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais

        {% else %}
            select
                cast(null as string) as id_transacao_recebida,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_transacao_recebida)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_transacao_recebida)
    ),
    transacao_erro_colunas_controle as (
        select
            * except (
                sha_dado_novo,
                sha_dado_atual,
                datetime_ultima_atualizacao_atual,
                id_execucao_dbt_atual,
                priority
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
from transacao_erro_colunas_controle
