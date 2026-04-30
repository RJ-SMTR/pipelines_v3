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

with
    autuacao_disciplinar_staging as (
        select
            data_infracao as data,
            datetime_infracao as datetime_autuacao,
            id_auto_infracao,
            id_infracao,
            modo,
            servico,
            permissao,
            placa,
            valor,
            status,
            data_pagamento,
            date(data) as data_inclusao_stu,
            current_date("America/Sao_Paulo") as data_inclusao_datalake,
            timestamp_captura
        from {{ ref("staging_infracao") }}
        {% if is_incremental() %}
            where
                date(data) between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )

        {% endif %}
    ),
    aux_data_inclusao as (
        select
            id_auto_infracao,
            min(data_inclusao_stu) as data_inclusao_stu,
            min(data_inclusao_datalake) as data_inclusao_datalake
        from
            {% if is_incremental() %}
                (
                    select id_auto_infracao, data_inclusao_stu, data_inclusao_datalake
                    from {{ this }}
                    union all
                    select id_auto_infracao, data_inclusao_stu, data_inclusao_datalake
                    from autuacao_disciplinar_staging
                )
            {% else %} autuacao_disciplinar_staging
            {% endif %}
        group by 1
    ),
    dados_novos as (
        select
            ad.* except (data_inclusao_stu, data_inclusao_datalake, timestamp_captura),
            di.data_inclusao_stu,
            di.data_inclusao_datalake,
            '{{ var("version") }}' as versao,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            '{{ invocation_id }}' as id_execucao_dbt
        from autuacao_disciplinar_staging ad
        join aux_data_inclusao di using (id_auto_infracao)
        qualify
            row_number() over (
                partition by id_auto_infracao order by timestamp_captura desc
            )
            = 1
    )
{% if is_incremental() %}
        ,
        dados_completos as (
            select *, 'tratada' as fonte, 0 as ordem
            from {{ this }}
            union all by name
            select *, 'staging' as fonte, 1 as ordem
            from dados_novos
        ),
        dados_completos_sha as (
            {% set columns = (
                list_columns()
                | reject(
                    "in",
                    [
                        "versao",
                        "datetime_ultima_atualizacao",
                        "id_execucao_dbt",
                    ],
                )
                | list
            ) %}

            select
                *,
                sha256(
                    concat(
                        {% for c in columns %}
                            ifnull(cast({{ c }} as string), 'n/a')
                            {% if not loop.last %}, {% endif %}
                        {% endfor %}
                    )
                ) as sha_dado
            from dados_completos
        )
    select * except (sha_dado)
    from dados_completos_sha
    qualify
        (
            fonte = 'tratada'
            and (
                lead(sha_dado) over (win) = sha_dado
                or lead(sha_dado) over (win) is null
            )
        )
        or (
            fonte = 'staging'
            and (
                lag(sha_dado) over (win) != sha_dado or lag(sha_dado) over (win) is null
            )
        )
    window win as (partition by id_auto_infracao order by ordem)
{% else %} select * from dados_novos
{% endif %}
