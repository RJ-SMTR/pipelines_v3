{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data_ordem",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
        alias="ordem_pagamento_guardador_veiculo_dia",
    )
}}

{% set aux_verificacao_particao_captura_riorotativo = ref(
    "aux_verificacao_particao_captura_riorotativo"
) %}

{% set verificacao_guardador_veiculo_riorotativo = ref(
    "verificacao_guardador_veiculo_riorotativo"
) %}


{% set ordem_table_exists = table_exists(this) %}

{% if execute %}
    {% if ordem_table_exists %}
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

        {% set verificacao_partitions_query %}

            select distinct
                concat("'", particao, "'") as particao
            from
                (
                    select
                        array_concat_agg(particoes) as particoes
                    from
                        {{ aux_verificacao_particao_captura_riorotativo }}
                    where data between date("{{var('date_range_start')}}") - interval 1 day and date(
                    "{{var('date_range_end')}}"
                )
                ),
                unnest(particoes) as particao

        {% endset %}

        {% set verificacao_partitions = (
            run_query(verificacao_partitions_query).columns[0].values()
        ) %}

    {% else %}
        {% set sha_column %}
        cast(null as bytes)
        {% endset %}
    {% endif %}
{% endif %}

with
    dados_novos as (
        select
            date(datetime_inclusao_verificacao + interval 1 day) as data_ordem,
            to_hex(
                sha256(
                    concat(
                        date(datetime_inclusao_verificacao + interval 1 day),
                        '_',
                        cpf_guardador_veiculo
                    )
                )
            ) as id_ordem_pagamento_guardador_veiculo_dia,
            cpf_guardador_veiculo,
            count(1) as quantidade_verificacao_total,
            countif(indicador_verificacao_valida) as quantidade_verificacao_valida,
            countif(
                not indicador_verificacao_valida
            ) as quantidade_verificacao_invalida,
            sum(valor_repasse_guardador_veiculo) as valor_repasse_guardador_veiculo
        from {{ verificacao_guardador_veiculo_riorotativo }}
        where
            date(datetime_inclusao_verificacao) < current_date("America/Sao_Paulo")
            and {% if is_incremental() %}
                date(
                    datetime_inclusao_verificacao + interval 1 day
                ) between date("{{var('date_range_start')}}") and date(
                    "{{var('date_range_end')}}"
                )
                and {% if verificacao_partitions | length > 0 %}
                    data in ({{ verificacao_partitions | join(", ") }})
                {% else %} false
                {% endif %}
            {% else %} data <= current_date("America/Sao_Paulo")
            {% endif %}
        group by 1, 2, 3
    ),
    {% if ordem_table_exists %}
        dados_atuais as (
            select *
            from {{ this }}
            {% if is_incremental() %}
                where
                    data_ordem between date("{{var('date_range_start')}}") and date(
                        "{{var('date_range_end')}}"
                    )

            {% endif %}
        ),
        particoes_completas as (
            select
                ifnull(n.data_ordem, a.data_ordem) as data_ordem,
                ifnull(
                    n.id_ordem_pagamento_guardador_veiculo_dia,
                    a.id_ordem_pagamento_guardador_veiculo_dia
                ) as id_ordem_pagamento_guardador_veiculo_dia,
                ifnull(
                    n.cpf_guardador_veiculo, a.cpf_guardador_veiculo
                ) as cpf_guardador_veiculo,
                ifnull(
                    n.quantidade_verificacao_total, a.quantidade_verificacao_total
                ) as quantidade_verificacao_total,
                ifnull(
                    n.quantidade_verificacao_valida, a.quantidade_verificacao_valida
                ) as quantidade_verificacao_valida,
                ifnull(
                    n.quantidade_verificacao_invalida, a.quantidade_verificacao_invalida
                ) as quantidade_verificacao_invalida,
                ifnull(
                    n.valor_repasse_guardador_veiculo, a.valor_repasse_guardador_veiculo
                ) as valor_repasse_guardador_veiculo,
                ifnull(
                    a.datetime_inclusao, current_datetime("America/Sao_Paulo")
                ) as datetime_inclusao,
                case
                    when
                        a.id_ordem_pagamento_guardador_veiculo_dia is not null
                        and a.quantidade_verificacao_total
                        != n.quantidade_verificacao_total
                    then
                        {% if ignorar_diferenca_ordem %} 1 / 1
                        {% else %} 1 / 0
                        {% endif %}
                end as quantidade_total_divergente,
                case
                    when
                        a.id_ordem_pagamento_guardador_veiculo_dia is not null
                        and a.quantidade_verificacao_valida
                        != n.quantidade_verificacao_valida
                    then
                        {% if ignorar_diferenca_ordem %} 1 / 1
                        {% else %} 1 / 0
                        {% endif %}
                end as quantidade_valida_divergente,
                case
                    when
                        a.id_ordem_pagamento_guardador_veiculo_dia is not null
                        and a.valor_repasse_guardador_veiculo
                        != n.valor_repasse_guardador_veiculo
                    then
                        {% if ignorar_diferenca_ordem %} 1 / 1
                        {% else %} 1 / 0
                        {% endif %}
                end as valor_divergente,
                case
                    when
                        max(a.id_ordem_pagamento_guardador_veiculo_dia) over (
                            partition by ifnull(n.data_ordem, a.data_ordem)
                        )
                        is not null
                        and a.id_ordem_pagamento_guardador_veiculo_dia is null
                    then
                        {% if ignorar_diferenca_ordem %} 1 / 1
                        {% else %} 1 / 0
                        {% endif %}
                end novo_registro_ordem
            from dados_novos n
            full outer join
                dados_atuais a using (id_ordem_pagamento_guardador_veiculo_dia)
        ),
    {% else %}
        particoes_completas as (
            select
                *,
                current_datetime("America/Sao_Paulo") as datetime_inclusao,
                null as quantidade_total_divergente,
                null as quantidade_valida_divergente,
                null as valor_divergente,
                null as novo_registro_ordem
            from dados_novos
        ),
    {% endif %}
    sha_dados_novos as (
        select
            * except (
                quantidade_total_divergente,
                quantidade_valida_divergente,
                valor_divergente,
                novo_registro_ordem
            ),
            {{ sha_column }} as sha_dado_novo
        from particoes_completas
    ),
    sha_dados_atuais as (
        {% if ordem_table_exists %}

            select
                id_ordem_pagamento_guardador_veiculo_dia,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais

        {% else %}
            select
                cast(null as string) as id_ordem_pagamento_guardador_veiculo_dia,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_ordem_pagamento_guardador_veiculo_dia)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_ordem_pagamento_guardador_veiculo_dia)
    ),
    ordem_colunas_controle as (
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
from ordem_colunas_controle
