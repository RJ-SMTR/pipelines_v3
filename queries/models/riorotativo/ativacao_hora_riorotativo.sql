{{
    config(
        materialized="incremental",
        alias="ativacao_hora",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set ativacao_table = ref("ativacao_riorotativo") %}
{% if execute %}
    {% if is_incremental() %}
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
            select
                concat("'", parse_date("%Y%m%d", partition_id), "'") as data
            from
                `rj-smtr.{{ ativacao_table.schema }}.INFORMATION_SCHEMA.PARTITIONS`
            where
                table_name = "{{ ativacao_table.identifier }}"
                and partition_id != "__NULL__"
                and datetime(last_modified_time, "America/Sao_Paulo") between datetime("{{var('date_range_start')}}") and (datetime("{{var('date_range_end')}}"))
        {% endset %}

        {% set partitions = run_query(partitions_query) %}

        {% set partition_list = partitions.columns[0].values() %}
    {% else %}
        {% set sha_column %}
            cast(null as bytes)
        {% endset %}
    {% endif %}
{% endif %}

with
    ativacao_agg as (
        select
            data,
            extract(hour from datetime_inicio_periodo) as hora,
            id_area,
            count(distinct id_ativacao) as quantidade_ativacao,
        from {{ ativacao_table }}
        {% if is_incremental() %}
            where
                {% if partition_list | length > 0 %}
                    data in ({{ partition_list | join(", ") }})
                {% else %} false
                {% endif %}
        {% endif %}
        group by all
    ),
    sha_dados_novos as (select *, {{ sha_column }} as sha_dado_novo from ativacao_agg),
    sha_dados_atuais as (
        {% if is_incremental() %}

            select
                data,
                hora,
                id_area,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }}

        {% else %}
            select
                cast(null as date) as data,
                cast(null as integer) as hora,
                cast(null as string) as id_area,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (data, hora, id_area)
        from sha_dados_novos n
        left join sha_dados_atuais a using (data, hora, id_area)
    ),
    ativacao_hora_colunas_controle as (
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
from ativacao_hora_colunas_controle
