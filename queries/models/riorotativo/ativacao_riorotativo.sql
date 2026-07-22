{{
    config(
        materialized="incremental",
        alias="ativacao",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set incremental_filter %}
    ({{ generate_date_hour_partition_filter(var('date_range_start'), var('date_range_end')) }})
    and datetime_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

{% set staging_movimento_estacionamento_veiculo_riorotativo = ref(
    "staging_movimento_estacionamento_veiculo_riorotativo"
) %}

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
                        {% if c == "geo_point_ativacao" %}
                            ifnull(st_astext(geo_point_ativacao), 'n/a')
                        {% elif c in ["ids_perfil_funcionamento", "ids_perfil_funcionamento"] %}
                            ifnull(to_json_string({{ c }}), 'n/a')
                        {% else %}ifnull(cast({{ c }} as string), 'n/a')
                        {% endif %}

                        {% if not loop.last %}, {% endif %}

                    {% endfor %}
                )
            )
        {% endset %}

        {% set partitions_query %}
            with staging as (
                select
                    date(datetime_periodo_inicial) as datetime_periodo_inicial,
                    date(datetime_periodo_final) as datetime_periodo_final
                from {{ staging_movimento_estacionamento_veiculo_riorotativo }}
                where {{ incremental_filter }}
            )
            select distinct concat("'", date(datetime_periodo_inicial), "'") as data
            from staging

            union distinct

            select distinct concat("'", date(datetime_periodo_final), "'") as data
            from staging

        {% endset %}

        {% set partitions = run_query(partitions_query).columns[0].values() %}

    {% else %}
        {% set sha_column %}
        cast(null as bytes)
        {% endset %}
    {% endif %}
{% endif %}

with
    movimento_estacionamento_veiculo as (
        select
            * except (id_tipo_periodo),
            cast(id_tipo_periodo as integer) as id_tipo_periodo
        from {{ ref("staging_movimento_estacionamento_veiculo_riorotativo") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (
                partition by id_movimento_estacionamento_veiculo
                order by datetime_captura desc
            )
            = 1
    ),
    movimento_estacionamento_veiculo_desdobrado as (
        select
            * except (
                id_movimento_estacionamento_veiculo,
                datetime_periodo_inicial,
                datetime_periodo_final,
                valor_pago
            ),
            concat(
                id_movimento_estacionamento_veiculo, '_', numero_ativacao
            ) as id_movimento_estacionamento_veiculo,
            valor_pago / id_tipo_periodo as valor_pago,
            least(
                datetime_periodo_inicial + interval (numero_ativacao - 1) * 2 hour,
                datetime_periodo_final
            ) as datetime_periodo_inicial,
            if(
                cast(id_tipo_periodo as integer) = numero_ativacao,
                datetime_periodo_final,
                datetime_periodo_inicial + interval numero_ativacao * 2 hour
            ) as datetime_periodo_final

        from
            movimento_estacionamento_veiculo,
            unnest(
                array(select n from unnest(generate_array(1, id_tipo_periodo)) as n)
            ) as numero_ativacao
    ),
    estacionamento_veiculo as (
        select *, st_geogpoint(longitude, latitude) as geo_point_ativacao
        from {{ ref("staging_estacionamento_veiculo_riorotativo") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (
                partition by id_estacionamento_veiculo order by datetime_captura desc
            )
            = 1
    ),
    veiculo_cliente as (
        select
            id_veiculo,
            placa,
            unnest_motoristas.id_veiculo_cliente,
            unnest_motoristas.cpf_motorista
        from
            {{ ref("veiculo_riorotativo") }},
            unnest(
                array(
                    select as struct cpf_motorista, id_veiculo_cliente
                    from unnest(motoristas)
                )
            ) as unnest_motoristas
    ),
    ativacao as (
        select
            date(m.datetime_periodo_inicial) as data,
            m.id_movimento_estacionamento_veiculo,
            m.datetime_periodo_inicial as datetime_inicio_periodo,
            m.datetime_periodo_final as datetime_fim_periodo,
            m.id_estacionamento_veiculo,
            e.id_veiculo_cliente,
            vc.id_veiculo,
            vc.placa as placa_veiculo,
            vc.cpf_motorista,
            e.latitude,
            e.longitude,
            e.geo_point_ativacao,
            a.id_area,
            a.id_perfil_funcionamento as ids_perfil_funcionamento,
            a.data_inicio_vigencia as data_inicio_vigencia_area,
            a.data_fim_vigencia as data_fim_vigencia_area,
            m.id_tipo_periodo,
            m.datetime_pagamento,
            m.valor_pago as valor_pago_bruto,
            m.valor_pago * numeric "0.042" as valor_retido_jae,
            m.valor_pago * numeric "0.958" as valor_pago_liquido,
            m.datetime_inclusao,
            m.datetime_captura
        from movimento_estacionamento_veiculo_desdobrado as m
        left join estacionamento_veiculo as e using (id_estacionamento_veiculo)
        left join veiculo_cliente as vc using (id_veiculo_cliente)
        join
            {{ ref("area_estacionamento_riorotativo") }} a
            on st_dwithin(e.geo_point_ativacao, a.geometry, 1000)
        qualify
            row_number() over (
                partition by m.id_movimento_estacionamento_veiculo
                order by st_distance(e.geo_point_ativacao, st_centroid(a.geometry)) asc
            )
            = 1
    ),
    {% if is_incremental() %}

        dados_atuais as (
            select *
            from {{ this }}
            where
                {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
                {% else %} false
                {% endif %}

        ),
    {% endif %}
    particoes_completas as (
        select *, 0 as priority
        from ativacao

        {% if is_incremental() %}
            union all

            select
                * except (versao, datetime_ultima_atualizacao, id_execucao_dbt),
                1 as priority
            from dados_atuais
            where
                split(id_movimento_estacionamento_veiculo, "_")[0] not in (
                    select split(id_movimento_estacionamento_veiculo, "_")[0]
                    from ativacao
                )
        {% endif %}
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo from particoes_completas
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}

            select
                id_movimento_estacionamento_veiculo,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais

        {% else %}
            select
                cast(null as string) as id_movimento_estacionamento_veiculo,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_movimento_estacionamento_veiculo)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_movimento_estacionamento_veiculo)
    ),
    ativacao_colunas_controle as (
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
from ativacao_colunas_controle
