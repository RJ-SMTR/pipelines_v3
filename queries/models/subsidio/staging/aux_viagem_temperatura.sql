{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date_add(date("{{ var('date_range_end') }}"), interval 1 day) and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset -%}

{% set partition_filter %}
    data between date("{{var('date_range_start')}}") and date("{{ var('date_range_end') }}") and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset %}

{% if execute %}
    {% if is_incremental() %}
        {% set columns = (
            list_columns()
            | reject(
                "in",
                [
                    "indicadores",
                    "versao",
                    "datetime_ultima_atualizacao",
                    "id_execucao_dbt",
                ],
            )
            | list
        ) + ["indicadores_str"] %}
        {% set sha_column %}
            sha256(
                concat(
                    {% for c in columns %}
                        {% if c == 'indicadores_str' %}
                            ifnull(
                                regexp_replace(
                                    cast({{ c }} as string),
                                    r'"datetime_verificacao_regularidade":"[^"]*",',
                                    ''
                                ),
                                'n/a'
                            )
                        {% else %}
                            ifnull(cast({{ c }} as string), 'n/a')
                        {% endif %}

                        {% if not loop.last %}, {% endif %}
                    {% endfor %}
                )
            )
        {% endset %}

        {% set partitions_query %}
            select distinct concat("'", data, "'") as data
            from {{ ref("viagem_classificada") }}
            where {{ partition_filter }}
        {% endset %}

        {% set partitions = run_query(partitions_query).columns[0].values() %}

    {% else %} {% set sha_column = "cast(null as bytes)" %}
    {% endif %}
{% endif %}

with
    indicadores_concatenados as (
        select * from {{ ref("eph_viagem_temperatura") }}
    ),
    {% if is_incremental() %}
        dados_atuais as (
            select
                * except (indicadores), to_json_string(indicadores) as indicadores_str,
            from {{ this }}
            {# from `rj-smtr`.`subsidio_staging`.`aux_viagem_temperatura` #}
            where
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} 1 = 0
                {% endif %}
        ),
    {% endif %}
    particoes_completas as (
        select *
        from indicadores_concatenados
        {% if is_incremental() %}
            union all by name

            select
                da.* except (versao, datetime_ultima_atualizacao, id_execucao_dbt),
                1 as priority
            from dados_atuais as da
            inner join indicadores_concatenados using (data, id_viagem)  -- Dados atuais só são incluídos se ainda existem nos dados novos
        {% endif %}
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo
        from particoes_completas
        qualify row_number() over (partition by data, id_viagem order by priority) = 1
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}
            select
                data,
                id_viagem,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais
        {% else %}
            select
                date(null) as data,
                cast(null as string) as id_viagem,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (data, id_viagem)
        from sha_dados_novos n
        left join sha_dados_atuais a using (data, id_viagem)
    ),
    struct_indicadores as (  -- Define datetime_verificacao_atual
        select
            * except (
                sha_dado_novo,
                sha_dado_atual,
                datetime_ultima_atualizacao_atual,
                id_execucao_dbt_atual,
                priority
            ),
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then current_datetime("America/Sao_Paulo")
                else datetime_ultima_atualizacao_atual
            end as datetime_verificacao_atual,
            case
                when sha_dado_atual is null or sha_dado_novo != sha_dado_atual
                then '{{ invocation_id }}'
                else id_execucao_dbt_atual
            end as id_execucao_dbt
        from sha_dados_completos
    ),
    colunas_controle as (
        select
            data,
            id_viagem,
            id_veiculo,
            id_validador,
            placa,
            ano_fabricacao,
            datetime_partida,
            datetime_chegada,
            modo,
            tecnologia_apurada,
            tecnologia_remunerada,
            tipo_viagem,
            quantidade_pre_tratamento,
            quantidade_nula,
            quantidade_zero,
            quantidade_pos_tratamento,
            parse_json(
                regexp_replace(
                    indicadores_str,
                    r'"datetime_verificacao_regularidade":"[^"]*",',
                    concat(
                        '"datetime_verificacao_regularidade":"',
                        datetime_verificacao_atual,
                        '",'
                    )
                )
            ) as indicadores,
            servico,
            sentido,
            distancia_planejada,
            '{{ var("version") }}' as versao,
            datetime_verificacao_atual as datetime_ultima_atualizacao,
            id_execucao_dbt
        from struct_indicadores as s
    )
select *
from colunas_controle
