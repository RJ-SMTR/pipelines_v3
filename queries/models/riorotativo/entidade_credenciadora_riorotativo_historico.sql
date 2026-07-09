{{ config(materialized="table", alias="entidade_credenciadora_historico") }}

{% set sha_versao %}
    sha256(concat(ifnull(razao_social, 'n/a'), '-', ifnull(nome_fantasia, 'n/a')))
{% endset %}

{% set sha_dados %}
    sha256(
        concat(
            ifnull(cast(data_fim_vigencia as string), 'n/a'),
            '-',
            ifnull(razao_social, 'n/a'),
            '-',
            ifnull(nome_fantasia, 'n/a')
        )
    )
{% endset %}

-- depends_on: {{ ref('staging_agente_credenciado_riorotativo') }}
{% if execute %}
    {% set cnpj_partitions_query %}
        select distinct cast(cnpj as int64)
        from {{ ref("staging_agente_credenciado_riorotativo") }}
        where cnpj is not null
    {% endset %}
    {% set cnpj_partitions = (
        run_query(cnpj_partitions_query).columns[0].values() | unique | list
    ) %}
{% endif %}

with
    dados_novos as (
        select
            current_date("America/Sao_Paulo") as data_inicio_vigencia,
            cnpj,
            razao_social,
            nome_fantasia
        from {{ source("rmi_dados_mestres", "pessoa_juridica") }}
        where
            cnpj_particao
            in ({{ cnpj_partitions | join(", ") if cnpj_partitions else "null" }})
    ),
    {% if table_exists(this) %} dados_atuais as (select * from {{ this }}), {% endif %}
    dados_completos as (
        select *, 0 as priority
        from dados_novos

        {% if table_exists(this) %}
            union all by name

            select
                data_inicio_vigencia, cnpj, razao_social, nome_fantasia, 1 as priority
            from dados_atuais
        {% endif %}
    ),
    dados_completos_deduplicados as (
        select *
        from dados_completos
        qualify
            row_number() over (
                partition by data_inicio_vigencia, cnpj order by priority
            )
            = 1
    ),
    entidade_credenciadora_mudanca_valor as (
        select
            * except (priority),
            {{ sha_versao }} != ifnull(
                lag({{ sha_versao }}) over (
                    partition by cnpj order by data_inicio_vigencia
                ),
                cast("" as bytes)
            ) as indicador_mudanca_valor
        from dados_completos_deduplicados
    ),
    entidade_credenciadora_data_fim_vigencia as (
        select
            * except (indicador_mudanca_valor),
            date_sub(
                lead(data_inicio_vigencia) over (
                    partition by cnpj order by data_inicio_vigencia
                ),
                interval 1 day
            ) as data_fim_vigencia
        from entidade_credenciadora_mudanca_valor
        where indicador_mudanca_valor
    ),
    sha_dados_novos as (
        select *, {{ sha_dados }} as sha_dado_novo
        from entidade_credenciadora_data_fim_vigencia
    ),
    sha_dados_atuais as (
        {% if table_exists(this) %}

            select
                data_inicio_vigencia,
                cnpj,
                {{ sha_dados }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from {{ this }}

        {% else %}
            select
                cast(null as date) as data_inicio_vigencia,
                cast(null as string) as cnpj,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (data_inicio_vigencia, cnpj)
        from sha_dados_novos as n
        left join sha_dados_atuais as a using (data_inicio_vigencia, cnpj)
    ),
    entidade_credenciadora_colunas_controle as (
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
from entidade_credenciadora_colunas_controle
