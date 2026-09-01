{{
    config(
        materialized="incremental",
        alias="verificacao_guardador_veiculo",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
    )
}}

{% set incremental_filter %}
    ({{ generate_date_hour_partition_filter(var('date_range_start'), var('date_range_end')) }})
    and datetime_captura between datetime("{{var('date_range_start')}}") and datetime("{{var('date_range_end')}}")
{% endset %}

{% set aux_verificacao_particao_captura_riorotativo = ref(
    "aux_verificacao_particao_captura_riorotativo"
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
                        {% if c in ["geo_point_verificacao"] %}
                            ifnull(st_astext({{ c }}), 'n/a')
                        {% elif c in ["ids_perfil_funcionamento", "ids_perfil_funcionamento", "cnpjs_entidade", "valor_repasse_entidades"] %}
                            ifnull(to_json_string({{ c }}), 'n/a')
                        {% else %}ifnull(cast({{ c }} as string), 'n/a')
                        {% endif %}

                        {% if not loop.last %}, {% endif %}

                    {% endfor %}
                )
            )
        {% endset %}
        {% set partitions_query %}
            with datas as (
                select distinct
                    particao
                from
                    (
                        select
                            array_concat_agg(particoes) as particoes
                        from
                            {{ aux_verificacao_particao_captura_riorotativo }}
                        where {{ incremental_filter }}
                    ),
                    unnest(particoes) as particao
            )
            select distinct
                concat("'", particao, "'") as data,
                concat("'", date(particao - interval 1 day), "'") as data_anterior,
                concat("'", date(particao + interval 1 day), "'") as data_posterior
            from datas

        {% endset %}

        {% set partitions_result = run_query(partitions_query) %}

        {% set verificacao_partitions = partitions_result.columns[0].values() %}

        {% set ativacao_partitions = (
            (
                partitions_result.columns[0].values()
                + partitions_result.columns[1].values()
                + partitions_result.columns[2].values()
            )
            | unique
            | list
        ) %}

    {% else %}
        {% set sha_column %}
        cast(null as bytes)
        {% endset %}
    {% endif %}
{% endif %}

with
    fiscalizacao_staging as (
        select *, st_geogpoint(longitude, latitude) as geo_point_verificacao
        from {{ ref("staging_fiscalizacao_veiculo_riorotativo") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (
                partition by id_fiscalizacao_veiculo order by datetime_captura desc
            )
            = 1
    ),
    ativacao as (
        select
            id_ativacao,
            ifnull(
                greatest(
                    datetime_inicio_periodo - interval 30 minute,
                    lag(datetime_fim_periodo) over (
                        partition by id_veiculo order by datetime_inicio_periodo
                    )
                ),
                datetime_inicio_periodo - interval 30 minute
            ) as datetime_inicio_periodo_tolerancia,
            datetime_inicio_periodo,
            datetime_fim_periodo,
            id_veiculo,
            placa_veiculo,
            cpf_motorista,
            valor_pago_bruto,
            valor_retido_jae,
            geo_point_ativacao
        from {{ ref("ativacao_riorotativo") }}
        {% if is_incremental() %}
            where
                {% if ativacao_partitions | length > 0 %}
                    data in ({{ ativacao_partitions | join(", ") }})
                {% else %} false
                {% endif %}
        {% endif %}
    ),
    guardador as (
        select data, documento, status, array_agg(cnpj) as cnpjs_entidade
        from {{ ref("guardador_veiculo_riorotativo_historico") }}
        {% if is_incremental() %}
            where
                {% if verificacao_partitions | length > 0 %}
                    data in ({{ verificacao_partitions | join(", ") }})
                {% else %} false
                {% endif %}
        {% endif %}
        group by all
    ),
    area_estacionamento as (
        select
            a.id_area,
            a.geometry,
            st_centroid(a.geometry) as centroide,
            a.data_inicio_vigencia,
            a.data_fim_vigencia,
            a.id_perfil_funcionamento as ids_perfil_funcionamento
        from {{ ref("area_estacionamento_riorotativo") }} a
    ),
    verificacao as (
        select
            date(f.data_fiscalizacao) as data,
            f.id_fiscalizacao_veiculo as id_verificacao,
            f.data_fiscalizacao as datetime_verificacao,
            f.datetime_inclusao as datetime_inclusao_verificacao,
            a.datetime_inicio_periodo as datetime_inicio_periodo_ativacao,
            a.datetime_fim_periodo as datetime_fim_periodo_ativacao,
            f.cpf_guardador_veiculo,
            g.cnpjs_entidade,
            g.status as status_guardador,
            a.cpf_motorista,
            ifnull(f.placa_digitada, f.placa_ocr) as placa_veiculo,
            f.placa_ocr as placa_veiculo_ocr,
            f.placa_digitada as placa_veiculo_digitada,
            f.id_veiculo,
            f.latitude,
            f.longitude,
            f.geo_point_verificacao,
            a.id_ativacao,
            a.geo_point_ativacao,
            a.valor_pago_bruto,
            a.valor_retido_jae
        from fiscalizacao_staging f
        left join
            ativacao a
            on (
                f.id_veiculo = a.id_veiculo
                or ifnull(f.placa_digitada, f.placa_ocr) = a.placa_veiculo
            )
            and f.data_fiscalizacao >= a.datetime_inicio_periodo_tolerancia
            and f.data_fiscalizacao < a.datetime_fim_periodo
        left join
            guardador g
            on f.cpf_guardador_veiculo = g.documento
            and date(f.data_fiscalizacao) = g.data
    ),
    verificacao_join_area_estacionamento as (
        select *
        from
            (
                select
                    v.data,
                    v.id_verificacao,
                    v.datetime_verificacao,
                    a.id_area,
                    data
                    between a.data_inicio_vigencia and a.data_fim_vigencia
                    as indicador_vaga_vigente,
                    a.ids_perfil_funcionamento,
                    0 as priority
                from verificacao v
                join
                    area_estacionamento a
                    on st_dwithin(v.geo_point_verificacao, a.geometry, 50)
                qualify
                    row_number() over (
                        partition by v.id_verificacao
                        order by
                            st_distance(
                                v.geo_point_verificacao, st_centroid(a.geometry)
                            ) asc
                    )
                    = 1

                union all

                select
                    v.data,
                    v.id_verificacao,
                    v.datetime_verificacao,
                    a.id_area,
                    data
                    between a.data_inicio_vigencia and a.data_fim_vigencia
                    as indicador_vaga_vigente,
                    a.ids_perfil_funcionamento,
                    1 as priority
                from verificacao v
                join
                    area_estacionamento a
                    on st_dwithin(v.geo_point_ativacao, a.geometry, 50)
                qualify
                    row_number() over (
                        partition by v.id_verificacao
                        order by
                            st_distance(
                                v.geo_point_ativacao, st_centroid(a.geometry)
                            ) asc
                    )
                    = 1
            )
        qualify row_number() over (partition by id_verificacao order by priority) = 1
    ),
    verificacao_join_area_perfil as (
        select
            v.id_verificacao,
            v.id_area,
            indicador_vaga_vigente,
            max(
                extract(dayofweek from v.data) in unnest(p.dias_semana)
                and time(v.datetime_verificacao)
                between p.horario_inicio and p.horario_fim
            ) as indicador_vaga_perfil_funcionamento_ativo
        from verificacao_join_area_estacionamento v
        left join
            {{ ref("perfil_funcionamento_riorotativo") }} p
            on p.id_perfil_funcionamento in unnest(v.ids_perfil_funcionamento)
        group by all

    ),
    {% if is_incremental() %}

        dados_atuais as (
            select *
            from {{ this }}
            where
                {% if ativacao_partitions | length > 0 %}
                    data in ({{ ativacao_partitions | join(", ") }})
                {% else %} false
                {% endif %}

        ),
    {% endif %}
    verificacao_indicador_ja_verificado as (
        select
            id_verificacao,
            (
                ifnull(
                    lag(id_ativacao) over (
                        partition by id_veiculo order by datetime_verificacao
                    ),
                    ""
                )
                = id_ativacao
            ) as indicador_ja_verificado
        from
            (
                select
                    data,
                    id_verificacao,
                    datetime_verificacao,
                    datetime_inclusao_verificacao,
                    id_veiculo,
                    id_ativacao
                from verificacao

                {% if is_incremental() %}
                    union all
                    select
                        data,
                        id_verificacao,
                        datetime_verificacao,
                        datetime_inclusao_verificacao,
                        id_veiculo,
                        id_ativacao
                    from dados_atuais
                    where id_verificacao not in (select id_verificacao from verificacao)
                {% endif %}

            )
        where data >= date(datetime_inclusao_verificacao)
    ),
    verificacao_validacao as (
        select
            v.*,
            vap.* except (id_verificacao),
            case
                when v.id_ativacao is null
                then "Rotativo não ativado"
                when vijv.indicador_ja_verificado
                then "Veículo já verificado neste período"
                when
                    vap.indicador_vaga_perfil_funcionamento_ativo is not null
                    and not vap.indicador_vaga_perfil_funcionamento_ativo
                then "Fora do horário de funcionamento do rotativo"
                when
                    vap.indicador_vaga_vigente is not null
                    and not vap.indicador_vaga_vigente
                then "Vaga inativa"
                when vap.id_area is null
                then "Verificação fora da área de estacionamento"
                when data < date(datetime_inclusao_verificacao)
                then "Envio fora do prazo diário (após 23h59)"
            end as motivo_nao_repasse
        from verificacao v
        join verificacao_indicador_ja_verificado vijv using (id_verificacao)
        left join verificacao_join_area_perfil vap using (id_verificacao)
    ),
    verificacao_repasse as (
        select
            *,
            motivo_nao_repasse is null as indicador_verificacao_valida,
            if(
                motivo_nao_repasse is null, numeric "1.40", numeric "0.0"
            ) as valor_repasse_guardador_veiculo,
            if(
                motivo_nao_repasse is null,
                [
                    struct(
                        "Sindicato" as entidade,
                        "34152025000122" as cnpj,
                        numeric "0.11" as valor_repasse
                    ),
                    struct(
                        "Associação" as entidade,
                        "05019730000158" as cnpj,
                        numeric "0.11" as valor_repasse
                    )
                ],
                [
                    struct(
                        "Sindicato" as entidade,
                        "34152025000122" as cnpj,
                        numeric "0.0" as valor_repasse
                    ),
                    struct(
                        "Associação" as entidade,
                        "05019730000158" as cnpj,
                        numeric "0.0" as valor_repasse
                    )
                ]
            ) as valor_repasse_entidades
        from verificacao_validacao
    ),
    verificacao_repasse_pcrj as (
        select
            *,
            (
                valor_pago_bruto
                - valor_retido_jae
                - valor_repasse_guardador_veiculo
                - (select sum(e.valor_repasse) from unnest(valor_repasse_entidades) e)
            ) as valor_repasse_pcrj
        from verificacao_repasse
    ),
    dados_novos as (
        select
            data,
            id_verificacao,
            datetime_verificacao,
            datetime_inclusao_verificacao,
            datetime_inicio_periodo_ativacao,
            datetime_fim_periodo_ativacao,
            cpf_guardador_veiculo,
            cnpjs_entidade,
            status_guardador,
            cpf_motorista,
            placa_veiculo,
            placa_veiculo_ocr,
            placa_veiculo_digitada,
            id_veiculo,
            latitude,
            longitude,
            geo_point_verificacao,
            id_area,
            indicador_vaga_vigente,
            indicador_vaga_perfil_funcionamento_ativo,
            id_ativacao,
            indicador_verificacao_valida,
            motivo_nao_repasse,
            valor_pago_bruto,
            valor_retido_jae,
            valor_repasse_guardador_veiculo,
            valor_repasse_entidades,
            valor_repasse_pcrj
        from verificacao_repasse_pcrj
    ),
    particoes_completas as (
        select *, 0 as priority
        from dados_novos

        {% if is_incremental() %}
            union all

            select
                * except (versao, datetime_ultima_atualizacao, id_execucao_dbt),
                1 as priority
            from dados_atuais
            {% if verificacao_partitions | length > 0 %}
                where data in ({{ verificacao_partitions | join(", ") }})
            {% endif %}

        {% endif %}
    ),
    sha_dados_novos as (
        select *, {{ sha_column }} as sha_dado_novo
        from particoes_completas
        qualify row_number() over (partition by id_verificacao order by priority) = 1
    ),
    sha_dados_atuais as (
        {% if is_incremental() %}

            select
                id_verificacao,
                {{ sha_column }} as sha_dado_atual,
                datetime_ultima_atualizacao as datetime_ultima_atualizacao_atual,
                id_execucao_dbt as id_execucao_dbt_atual
            from dados_atuais

        {% else %}
            select
                cast(null as string) as id_verificacao,
                cast(null as bytes) as sha_dado_atual,
                datetime(null) as datetime_ultima_atualizacao_atual,
                cast(null as string) as id_execucao_dbt_atual
        {% endif %}
    ),
    sha_dados_completos as (
        select n.*, a.* except (id_verificacao)
        from sha_dados_novos n
        left join sha_dados_atuais a using (id_verificacao)
    ),
    verificacao_colunas_controle as (
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
from verificacao_colunas_controle
