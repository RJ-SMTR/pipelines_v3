{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data_ordem",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
date(data) between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

{% set transacao_ordem = ref("staging_transacao_ordem") %}
{% set aux_transacao_id_ordem_pagamento = ref("aux_transacao_id_ordem_pagamento") %}
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

            select distinct concat("'", date(data_transacao), "'") as data_transacao
            from {{ transacao_ordem }}
            where {{ incremental_filter }}

        {% endset %}

        {% set partitions = run_query(partitions_query).columns[0].values() %}

        {% set data_ordem_partitions_query %}

            select distinct concat("'", data_ordem , "'") as data_ordem
            from {{ aux_transacao_id_ordem_pagamento }}
            where
                {% if partitions | length > 0 %}
                    data in ({{ partitions | join(", ") }})
                {% else %} 1 = 0
                {% endif %}
        {% endset %}

        {% set data_ordem_partitions = (
            run_query(data_ordem_partitions_query).columns[0].values()
        ) %}

    {% else %} {% set sha_column = "cast(null as bytes)" %}
    {% endif %}
{% endif %}

with
    dados_novos as (
        select distinct
            data_ordem,
            id_ordem_pagamento_consorcio_operador_dia,
            data as data_transacao
        from {{ aux_transacao_id_ordem_pagamento }}
        {% if is_incremental() %}
            where
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} 1 = 0
                {% endif %}
        {% endif %}
    ),
    {% if is_incremental() %}
        dados_atuais as (
            select *
            from {{ this }}
            where
                {% if data_ordem_partitions | length > 0 %}
                    data_ordem in ({{ data_ordem_partitions | join(", ") }})
                {% else %} 1 = 0
                {% endif %}
        ),
    {% endif %}
    particoes_completas as (
        select *, 0 as priority
        from dados_novos

        {% if is_incremental() %}
            union all

            select
                * except (versao, datetime_ultima_atualizacao, id_execucao_dbt),
                1 as priority
            from dados_atuais
        {% endif %}
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo
        from particoes_completas
        qualify
            row_number() over (
                partition by
                    data_ordem,
                    id_ordem_pagamento_consorcio_operador_dia,
                    data_transacao
                order by priority
            )
            = 1
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}
            select
                data_ordem,
                id_ordem_pagamento_consorcio_operador_dia,
                data_transacao,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais
        {% else %}
            select
                date(null) as data_ordem,
                cast(null as string) as id_ordem_pagamento_consorcio_operador_dia,
                date(null) as data_transacao,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select
            n.*,
            a.* except (
                data_ordem, id_ordem_pagamento_consorcio_operador_dia, data_transacao
            )
        from sha_dados_novos n
        left join
            sha_dados_atuais a using (
                data_ordem, id_ordem_pagamento_consorcio_operador_dia, data_transacao
            )
    ),
    colunas_controle as (
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
from colunas_controle
