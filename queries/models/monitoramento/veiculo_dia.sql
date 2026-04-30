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
    licenciamento as (
        select
            *,
            case
                when
                    date(data) between ('2026-01-01') and ('2026-01-31')
                    and ano_ultima_vistoria >= extract(year from date(data)) - 2
                then true  -- RESOLUÇÃO SMTR Nº 3894 DE 29 DE DEZEMBRO DE 2025 que altera o prazo final de vistoria para 31 de janeiro de 2026
                when
                    ano_ultima_vistoria >= cast(
                        extract(
                            year
                            from
                                date_sub(
                                    date(data),
                                    interval {{
                                        var(
                                            "sppo_licenciamento_validade_vistoria_ano"
                                        )
                                    }} year
                                )
                        ) as int64
                    )
                then true  -- Última vistoria realizada dentro do período válido
                when
                    data_ultima_vistoria is null
                    and date_diff(date(data), data_inicio_vinculo, day)
                    <= {{ var("sppo_licenciamento_tolerancia_primeira_vistoria_dia") }}
                then true  -- Caso o veículo seja novo, existe a tolerância de 15 dias para a primeira vistoria
                else false
            end as indicador_vistoriado,
        from {{ ref("veiculo_licenciamento_dia") }}
        where
            (
                data_processamento <= date_add(data, interval 7 day)
                or data_processamento in (
                    {{
                        var("data_processamento_veiculo_licenciamento_dia") | join(
                            ", "
                        )
                    }}
                )
                or (
                    data between "2025-07-16" and "2025-07-31"  -- Exceção para lacres adicionados após o prazo em 2025-07-Q2
                    and data_processamento between "2025-07-16" and "2025-08-13"
                )
                or (
                    data between "2025-08-01" and "2025-08-18"  -- Exceção para falha do arquivo de licenciamento no ftp
                    and data_processamento between "2025-08-01" and "2025-08-26"
                )
                or (
                    data between "2025-09-01" and "2025-09-25"  -- Exceção para lacres adicionados após o prazo em 2025-09-Q1
                    and data_processamento between "2025-09-01" and "2025-09-25"
                )
                or (  -- Exceção para tratamento da data_ultima_vistoria [Troca placa Mercosul]
                    data_processamento between "2025-07-10" and "2025-12-04"
                    and (
                        (
                            data between "2025-07-10" and "2025-07-20"
                            and id_veiculo = "B58188"
                        )
                        or (
                            data between "2025-07-30" and "2025-08-31"
                            and id_veiculo = "A29139"
                        )
                    )
                )
                or (
                    data between "2025-11-12" and "2025-12-01"  -- Exceção para ajuste na tecnologia MTR-CAP-2025/59482
                    and data_processamento between "2025-11-12" and "2025-12-22"
                )
                or (
                    data between "2025-11-20" and "2025-12-16"  -- Exceção para ajuste na tecnologia MTR-CAP-2025/59482
                    and data_processamento between "2025-11-20" and "2026-01-13"
                )
            )
            {% if is_incremental() %}
                and data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )

            {% endif %}
        qualify
            row_number() over (
                partition by data, id_veiculo order by data_processamento desc
            )
            = 1
    ),
    autuacao_disciplinar as (
        select *
        from {{ ref("autuacao_disciplinar_historico") }}
        where
            (
                data_inclusao_datalake <= date_add(data, interval 7 day)
                or data_inclusao_datalake
                = date("{{var('data_inclusao_autuacao_disciplinar')}}")  -- Primeira data de inclusão dos dados de autuações disciplinares
            )
            and status != "Cancelada"
            {% if is_incremental() %}
                and data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )

            {% endif %}
    ),
    gps as (
        select *
        from {{ ref("aux_veiculo_gps_dia") }}
        {% if is_incremental() %}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )

        {% endif %}
    ),
    registros_agente_verao as (
        select distinct data, id_veiculo
        from {{ ref("sppo_registro_agente_verao") }}
        {# from `rj-smtr.veiculo.sppo_registro_agente_verao` #}
        {% if is_incremental() %}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
        {% endif %}
    ),
    gps_licenciamento as (
        select
            data,
            id_veiculo,
            l.placa,
            l.modo,
            l.tecnologia,
            l.tipo_veiculo,
            l.ano_fabricacao,
            l.id_veiculo is not null as indicador_licenciado,
            l.indicador_vistoriado,
            l.indicador_ar_condicionado,
            l.indicador_veiculo_lacrado,
            r.id_veiculo is not null as indicador_registro_agente_verao_ar_condicionado,
            l.data_processamento as data_processamento_licenciamento,
            r.data as data_registro_agente_verao
        from gps g
        left join licenciamento as l using (data, id_veiculo)
        left join registros_agente_verao as r using (data, id_veiculo)
    ),
    autuacao_ar_condicionado as (
        select
            data,
            placa,
            data_inclusao_datalake as data_inclusao_datalake_autuacao_ar_condicionado
        from autuacao_disciplinar
        where id_infracao = "023.II"
        qualify
            row_number() over (
                partition by data, placa order by data_inclusao_datalake desc
            )
            = 1
    ),
    autuacao_completa as (
        select distinct
            data,
            placa,
            ac.data is not null as indicador_autuacao_ar_condicionado,
            ac.data_inclusao_datalake_autuacao_ar_condicionado,
        from autuacao_ar_condicionado ac
    ),
    gps_licenciamento_autuacao as (
        select
            data,
            gl.id_veiculo,
            placa,
            gl.modo,
            gl.tecnologia,
            gl.tipo_veiculo,
            gl.ano_fabricacao,
            struct(
                struct(
                    gl.indicador_licenciado as valor,
                    gl.data_processamento_licenciamento
                    as data_processamento_licenciamento
                ) as indicador_licenciado,
                struct(
                    gl.indicador_vistoriado as valor,
                    gl.data_processamento_licenciamento
                    as data_processamento_licenciamento
                ) as indicador_vistoriado,
                struct(
                    gl.indicador_ar_condicionado as valor,
                    gl.data_processamento_licenciamento
                    as data_processamento_licenciamento
                ) as indicador_ar_condicionado,
                struct(
                    gl.indicador_veiculo_lacrado as valor,
                    gl.data_processamento_licenciamento
                    as data_processamento_licenciamento
                ) as indicador_veiculo_lacrado,
                struct(
                    coalesce(a.indicador_autuacao_ar_condicionado, false) as valor,
                    a.data_inclusao_datalake_autuacao_ar_condicionado
                    as data_inclusao_datalake_autuacao
                ) as indicador_autuacao_ar_condicionado,
                struct(
                    gl.indicador_registro_agente_verao_ar_condicionado as valor,
                    gl.data_registro_agente_verao as data_registro_agente_verao

                ) as indicador_registro_agente_verao_ar_condicionado
            ) as indicadores
        from gps_licenciamento gl
        left join autuacao_completa as a using (data, placa)
    ),
    dados_novos as (
        select
            data,
            id_veiculo,
            placa,
            modo,
            tecnologia,
            tipo_veiculo,
            ano_fabricacao,
            case
                when indicadores.indicador_licenciado.valor is false
                then "Não licenciado"
                when indicadores.indicador_vistoriado.valor is false
                then "Não vistoriado"
                when indicadores.indicador_veiculo_lacrado.valor is true
                then "Lacrado"
                when
                    indicadores.indicador_ar_condicionado.valor is true
                    and indicadores.indicador_autuacao_ar_condicionado.valor is true
                then "Autuado por ar inoperante"
                when
                    indicadores.indicador_ar_condicionado.valor is true
                    and indicadores.indicador_registro_agente_verao_ar_condicionado.valor
                    is true
                then "Registrado com ar inoperante"
                when indicadores.indicador_ar_condicionado.valor is false
                then "Licenciado sem ar e não autuado"
                when indicadores.indicador_ar_condicionado.valor is true
                then "Licenciado com ar e não autuado"
            end as status,
            to_json(indicadores) as indicadores,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            "{{ var('version') }}" as versao,
            '{{ invocation_id }}' as id_execucao_dbt
        from gps_licenciamento_autuacao
    )
{% if is_incremental() %}
        ,
        dados_completos as (
            select *, 1 as ordem
            from dados_novos

            union all

            select *, 0 as ordem
            from {{ this }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )

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
            window win as (partition by data, id_veiculo order by ordem)
        )

    select * except (ordem)
    from dados_completos_invocation_id
    where ordem = 1
{% else %} select * from dados_novos
{% endif %}
