{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
    )
}}

{% set aux_operadora_stu = ref("aux_operadora_stu") %}

{% set aux_operadora_jae = ref("aux_operadora_jae") %}

{% set columns = [
    "cnpj",
    "razao_social",
    "nome_fantasia",
    "situacao_cadastral",
    "situacao_especial",
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

{% if is_incremental() %}
    {% set incremental_filter %}
            date(data)
            between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
            and timestamp_captura between datetime("{{var('date_range_start')}}") and datetime(
                "{{var('date_range_end')}}"
            )
            and tipo_documento = "CNPJ"
    {% endset %}

    {% set all_columns = (
        list_columns()
        | reject(
            "in",
            ["versao", "datetime_ultima_atualizacao", "id_execucao_dbt"],
        )
        | list
    ) %}

    {% set sha_all_column %}
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
    {% set incremental_filter %}
            tipo_documento = "CNPJ"
    {% endset %}

    {% set sha_all_column %}
        cast(null as bytes)
    {% endset %}

{% endif %}


{% if execute %}
    {% set partitions_query %}
        select cast(documento as integer)
        from
            (
                select documento
                from {{ aux_operadora_stu }}
                where {{ incremental_filter }}

                union distinct

                select documento
                from {{ aux_operadora_jae }}
                where {{ incremental_filter }}
            )
    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% endif %}

with
    dados_novos as (
        select
            current_datetime("America/Sao_Paulo") as datetime_inicio_validade,
            {{ columns | join(", ") }}
        from {{ source("rmi_dados_mestres", "pessoa_juridica") }}
        where
            {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
            {% else %} false
            {% endif %}
    ),
    {% if is_incremental() %} dados_atuais as (select * from {{ this }}), {% endif %}
    dados_completos as (
        select *, 0 as priority
        from dados_novos

        {% if is_incremental() %}
            union all by name

            select
                * except (
                    datetime_fim_validade,
                    versao,
                    datetime_ultima_atualizacao,
                    id_execucao_dbt
                ),
                1 as priority
            from dados_atuais
        {% endif %}
    ),
    operadora_mudanca_valor as (
        select
            * except (priority),
            {{ sha_column }} != ifnull(lag({{ sha_column }}), cast("" as bytes)) over (
                partition by cnpj order by datetime_inicio_validade, priority
            ) as indicador_mudanca_valor
        from dados_completos
    ),
    operadora_datetime_fim_validade as (
        select
            * except (indicador_mudanca_valor),
            lag(datetime_inicio_validade) over (
                partition by cnpj order by datetime_inicio_validade
            ) as datetime_fim_validade
        from operadora_mudanca_valor
        where indicador_mudanca_valor
    ),
    sha_dados_novos as (
        select *, {{ sha_all_column }} as sha_dado_novo
        from operadora_datetime_fim_validade
        qualify
            row_number() over (
                partition by id_unico_lancamento, id_conta
                order by priority, datetime_lancamento desc
            )
            = 1
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}

            select
                cnpj,
                {{ sha_all_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais

        {% else %}
            select
                cast(null as string) as cnpj,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (cnpj)
        from sha_dados_novos n
        left join sha_dados_atuais a using (cnpj)
    ),
    operadora_colunas_controle as (
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
from operadora_colunas_controle
