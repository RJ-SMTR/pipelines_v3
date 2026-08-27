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

{% set aux_ativacao_particao_captura_riorotativo = ref(
    "aux_ativacao_particao_captura_riorotativo"
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

            select distinct
                concat("'", particao, "'") as particao
            from
                (
                    select
                        array_concat_agg(particoes) as particoes
                    from
                        {{ aux_ativacao_particao_captura_riorotativo }}
                    where {{ incremental_filter }}
                ),
                unnest(particoes) as particao

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
        select *, st_geogpoint(longitude, latitude) as geo_point_ativacao
        from {{ ref("staging_movimento_estacionamento_veiculo_riorotativo") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (
                partition by id_movimento_estacionamento_veiculo
                order by datetime_captura desc
            )
            = 1
    ),
    tipo_periodo as (
        select *
        from
            unnest(
                [
                    struct(
                        '1' as id_tipo_periodo,
                        2 as quantidade_horas_periodo,
                        1 as quantidade_periodos
                    ),
                    struct(
                        '2' as id_tipo_periodo,
                        2 as quantidade_horas_periodo,
                        2 as quantidade_periodos
                    ),
                    struct(
                        '3' as id_tipo_periodo,
                        2 as quantidade_horas_periodo,
                        3 as quantidade_periodos
                    )
                ]
            )
    ),
    movimento_estacionamento_veiculo_tipo_periodo as (
        select m.*, t.* except (id_tipo_periodo)
        from movimento_estacionamento_veiculo m
        left join tipo_periodo t using (id_tipo_periodo)
    ),
    movimento_estacionamento_veiculo_desdobrado as (
        select
            * except (datetime_periodo_inicial, datetime_periodo_final, valor_pago),
            concat(
                id_movimento_estacionamento_veiculo, '_', numero_ativacao
            ) as id_ativacao,
            valor_pago / ifnull(quantidade_periodos, 0) as valor_pago,
            least(
                datetime_periodo_inicial
                + interval (numero_ativacao - 1) * quantidade_horas_periodo hour,
                datetime_periodo_final
            ) as datetime_periodo_inicial,
            if(
                cast(id_tipo_periodo as integer) = numero_ativacao,
                datetime_periodo_final,
                datetime_periodo_inicial
                + interval numero_ativacao * quantidade_horas_periodo hour
            ) as datetime_periodo_final
        from
            movimento_estacionamento_veiculo_tipo_periodo,
            unnest(
                array(
                    select n
                    from unnest(generate_array(1, ifnull(quantidade_periodos, 1))) as n
                )
            ) as numero_ativacao

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
            m.id_ativacao,
            m.datetime_periodo_inicial as datetime_inicio_periodo,
            m.datetime_periodo_final as datetime_fim_periodo,
            m.id_movimento_estacionamento_veiculo,
            m.id_estacionamento_veiculo,
            e.id_veiculo_cliente,
            vc.id_veiculo,
            vc.placa as placa_veiculo,
            vc.cpf_motorista,
            m.latitude,
            m.longitude,
            m.geo_point_ativacao,
            a.id_area,
            a.id_perfil_funcionamento as ids_perfil_funcionamento,
            m.id_tipo_periodo,
            m.datetime_pagamento,
            m.valor_pago as valor_pago_bruto,
            m.valor_pago * numeric "0.042" as valor_retido_jae,
            m.valor_pago * numeric "0.958" as valor_pago_liquido,
            m.datetime_inclusao,
            m.datetime_captura
        from movimento_estacionamento_veiculo_desdobrado as m
        left join veiculo_cliente as vc using (id_veiculo_cliente)
        join
            {{ ref("area_estacionamento_riorotativo") }} a
            on st_dwithin(e.geo_point_ativacao, a.geometry, 1000)
        qualify
            row_number() over (
                partition by m.id_ativacao
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
                d.* except (versao, datetime_ultima_atualizacao, id_execucao_dbt),
                1 as priority
            from dados_atuais d
            left join
                (select distinct id_movimento_estacionamento_veiculo from ativacao) a
                on d.id_movimento_estacionamento_veiculo
                = a.id_movimento_estacionamento_veiculo
            where a.id_movimento_estacionamento_veiculo is null
        {% endif %}
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo from particoes_completas
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}

            select
                id_ativacao,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais

        {% else %}
            select
                cast(null as string) as id_ativacao,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_ativacao)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_ativacao)
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
