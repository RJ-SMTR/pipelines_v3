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

{% set staging_fiscalizacao_veiculo_riorotativo = ref(
    "staging_fiscalizacao_veiculo_riorotativo"
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
                concat("'", date(data_fiscalizacao), "'") as data,
                concat("'", date(data_fiscalizacao - interval 1 day), "'") as data_anterior,
                concat("'", date(data_fiscalizacao + interval 1 day), "'") as data_posterior
            from {{ staging_fiscalizacao_veiculo_riorotativo }}
            {# where {{ incremental_filter }} #}

        {% endset %}

        {% set partitions_result = run_query(partitions_query) %}

        {% set verificacao_partitions = partitions_result.columns[0].values() %}
        {% set ativacao_partitions = (
            partitions_result.columns[0].values()
            + partitions_result.columns[1].values()
            + partitions_result.columns[2].values()
        ) | unique %}

    {% else %}
        {% set sha_column %}
        cast(null as bytes)
        {% endset %}
    {% endif %}
{% endif %}

with
    fiscalizacao_staging as (
        select *, st_geogpoint(longitude, latitude) as geo_point_verificacao
        from {{ staging_fiscalizacao_veiculo_riorotativo }}
        where {% if is_incremental() %} {{ incremental_filter }} {% endif %}
    ),
    ativacao as (
        select
            id_movimento_estacionamento_veiculo,
            datetime_inicio_periodo,
            datetime_fim_periodo,
            cpf_motorista,
            valor_pago_bruto,
            valor_retido_jae
        from {{ ref("ativacao_riorotativo") }}
        where
            placa_veiculo is not null
            {% if is_incremental() %}
                and {% if ativacao_partitions | length > 0 %}
                    data in ({{ ativacao_partitions | join(", ") }})
                {% else %} false
                {% endif %}
            {% endif %}
    ),
    verificacao as (
        select
            date(f.data_fiscalizacao) as data,
            f.id_fiscalizacao_veiculo as id_verificacao,
            f.data_fiscalizacao as datetime_verificacao,
            f.data_inclusao as datetime_inclusao_verificacao,
            f.cpf_guardador_veiculo,
            ifnull(f.placa_digitada, f.placa_ocr) as placa_veiculo,
            f.placa_ocr as placa_veiculo_ocr,
            f.placa_digitada as placa_veiculo_digitada,
            f.id_veiculo
            f.latitude,
            f.longitude,
            f.geo_point_verificacao
        from fiscalizacao_staging f
        left join ativacao a on f
    )
