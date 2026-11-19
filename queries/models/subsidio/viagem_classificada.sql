{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set viagem_valida = ref("viagem_valida") %}
{% if execute and is_incremental() %}
    {% set modified_partitions = get_modified_partitions_filter(
        viagem_valida,
        truncate_date=true,
        max_age_days=var("viagem_validacao_max_age_days", 6),
    ) %}
{% else %} {% set modified_partitions = [] %}
{% endif %}

{% set incremental_filter %}
    {% if var("flow_name") == "treatment--monitoramento-temperatura" %}
        (
            data between date("{{ var('date_range_start') }}") and date(
                "{{ var('date_range_end') }}"
            )
            {% if is_incremental() %}
                and data < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
            {% endif %}
            {% if is_incremental() and modified_partitions | length > 0 %}
                or (
                    data in ({{ modified_partitions | join(", ") }})
                    and data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
                )
            {% endif %}
        )
        and data >= date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
    {% else %}
        (
            data between date("{{ var('start_date') }}") and date(
                "{{ var('end_date') }}"
            )
            {% if is_incremental() and modified_partitions | length > 0 %}
                or (
                    data in ({{ modified_partitions | join(", ") }})
                    and data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
                )
            {% endif %}
        )
        and data >= date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
    {% endif %}
{% endset %}

with
    viagem_indicador as (
        select
            data,
            id_viagem,
            id_veiculo,
            placa,
            ano_fabricacao,
            datetime_partida,
            datetime_chegada,
            modo,
            tecnologia_apurada,
            tecnologia_remunerada,
            {{
                tipo_viagem_sppo(
                    "data",
                    "tipo_viagem",
                    "indicador_detectado_ar_inoperante",
                    sem_transacao="indicador_sem_transacao",
                    sem_transacao_tipo="indicador_sem_transacao_tipo",
                    estado_equipamento_aberto="indicador_estado_equipamento_aberto",
                    validador_fechado="indicador_validador_fechado",
                    validador_associado_incorretamente="indicador_validador_associado_incorretamente",
                )
            }}
            as tipo_viagem,
            indicadores,
            servico,
            sentido,
            distancia_planejada,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            "{{ var('version') }}" as versao,
            '{{ invocation_id }}' as id_execucao_dbt
        from {{ ref("aux_viagem_indicador") }}
        where {{ incremental_filter }} and sistema = "SPPO"
    ),
    deduplica as (select * from viagem_indicador)
{% if is_incremental() %}
        ,
        dados_completos as (
            select *, 1 as ordem
            from deduplica

            union all

            select *, 0 as ordem
            from {{ this }}
            where {{ incremental_filter }}

        ),
        sha_dados as (
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
                            ifnull(
                                {% if c == "indicadores" %}to_json_string(indicadores)
                                {% else %}cast({{ c }} as string)
                                {% endif %},
                                'n/a'
                            )
                            {% if not loop.last %}, {% endif %}
                        {% endfor %}
                    )
                ) as sha_dado
            from dados_completos
        ),
        dados_completos_invocation_id as (
            select
                * except (id_execucao_dbt, sha_dado),
                case
                    when
                        lag(sha_dado) over (win) != sha_dado
                        or (lag(sha_dado) over (win) is null and ordem = 1)
                    then id_execucao_dbt
                    else lag(id_execucao_dbt) over (win)
                end as id_execucao_dbt
            from sha_dados
            window win as (partition by data, id_viagem order by ordem)
        )

    select * except (ordem)
    from dados_completos_invocation_id
    where ordem = 1
{% else %} select * from deduplica
{% endif %}
