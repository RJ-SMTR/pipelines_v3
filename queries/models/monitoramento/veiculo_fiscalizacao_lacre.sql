{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data_inicio_lacre",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set regex_filter %} regexp_contains(no_do_auto, r'[/-]') {% endset %}

{% set incremental_filter %}
    date(data) between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
    and {{ regex_filter }}
{% endset %}

{% set staging_veiculo_fiscalizacao_lacre = ref("staging_veiculo_fiscalizacao_lacre") %}

{% if execute and is_incremental() %}
    {% set partitions_query %}

        SELECT DISTINCT
            CONCAT("'", data_do_lacre, "'") AS data_do_lacre
        FROM
            {{ staging_veiculo_fiscalizacao_lacre }}
        WHERE
            {{ incremental_filter }}
    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% endif %}

with
    staging as (
        select *
        from {{ staging_veiculo_fiscalizacao_lacre }}
        {% if is_incremental() %} where {{ incremental_filter }}
        {% else %} where {{ regex_filter }}
        {% endif %}
        qualify
            row_number() over (
                partition by n_o_de_ordem, placa, data_do_lacre, no_do_auto
                order by timestamp_captura desc
            )
            = 1
    ),
    particoes_completas as (
        select
            n_o_de_ordem as id_veiculo,
            placa,
            data_do_lacre as data_inicio_lacre,
            data_do_deslacre as data_fim_lacre,
            permissao as id_consorcio,
            consorcio,
            concat(
                rpad(regexp_replace(substring(no_do_auto, 1, 2), r'\W', ''), 2),
                '-',
                lpad(regexp_replace(substring(no_do_auto, 3), r'\W', ''), 8, '0')
            ) as id_auto_infracao,
            ultima_atualizacao as datetime_ultima_atualizacao_fonte,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            '{{ invocation_id }}' as id_execucao_dbt
        from staging

        {% if is_incremental() and partitions | length > 0 %}

                {% set max_data_query %}
                select max(data) as max_data
                from {{ staging_veiculo_fiscalizacao_lacre }}
                {% endset %}
                {% set max_data = run_query(max_data_query).columns[0].values()[0] %}

            union all

            select * except (versao)
            from {{ this }} as historico
            where
                data_inicio_lacre in ({{ partitions | join(", ") }})
                and exists (
                    select 1
                    from {{ staging_veiculo_fiscalizacao_lacre }} as s
                    where
                        s.data = date('{{ max_data }}')
                        and s.n_o_de_ordem = historico.id_veiculo
                        and s.placa = historico.placa
                        and s.data_do_lacre = historico.data_inicio_lacre
                        and regexp_contains(s.no_do_auto, r'/')
                        and concat(
                            rpad(
                                regexp_replace(
                                    substring(s.no_do_auto, 1, 2), r'\W', ''
                                ),
                                2
                            ),
                            '-',
                            lpad(
                                regexp_replace(substring(s.no_do_auto, 3), r'\W', ''),
                                8,
                                '0'
                            )
                        )
                        = historico.id_auto_infracao
                )

        {% endif %}
    ),
    aux_datetime_ultima_atualizacao as (
        select
            id_veiculo,
            placa,
            data_inicio_lacre,
            id_auto_infracao,
            case
                when
                    array_length(atualizacoes) = 1
                    or atualizacoes[0].datetime_ultima_atualizacao_fonte
                    = atualizacoes[1].datetime_ultima_atualizacao_fonte
                then atualizacoes[0].datetime_ultima_atualizacao
                else atualizacoes[1].datetime_ultima_atualizacao
            end as datetime_ultima_atualizacao,
            case
                when
                    array_length(atualizacoes) = 1
                    or atualizacoes[0].datetime_ultima_atualizacao_fonte
                    = atualizacoes[1].datetime_ultima_atualizacao_fonte
                then atualizacoes[0].id_execucao_dbt
                else atualizacoes[1].id_execucao_dbt
            end as id_execucao_dbt
        from
            (
                select
                    id_veiculo,
                    placa,
                    data_inicio_lacre,
                    id_auto_infracao,
                    array_agg(
                        struct(
                            datetime_ultima_atualizacao_fonte
                            as datetime_ultima_atualizacao_fonte,
                            datetime_ultima_atualizacao as datetime_ultima_atualizacao,
                            id_execucao_dbt as id_execucao_dbt
                        )
                        order by datetime_ultima_atualizacao
                    ) as atualizacoes
                from particoes_completas
                group by all
            )
    ),
    dados_completos as (
        select
            p.* except (datetime_ultima_atualizacao, id_execucao_dbt),
            a.datetime_ultima_atualizacao,
            '{{ var("version") }}' as versao,
            a.id_execucao_dbt
        from particoes_completas p
        join
            aux_datetime_ultima_atualizacao a using (
                id_veiculo, placa, data_inicio_lacre, id_auto_infracao
            )
        qualify
            row_number() over (
                partition by data_inicio_lacre, id_veiculo, placa, id_auto_infracao
                order by datetime_ultima_atualizacao_fonte desc
            )
            = 1
    ),
    lacre_atualizado as (
        select dc.*
        from {{ this }} as da
        inner join
            dados_completos as dc using (
                id_veiculo, placa, data_inicio_lacre, id_auto_infracao
            )
        where
            da.data_fim_lacre is distinct from dc.data_fim_lacre
            and dc.data_fim_lacre <= '{{ var("data_final_veiculo_arquitetura_1") }}'
    ),

    dados_finais as (
        select *
        from dados_completos
        where
            data_fim_lacre > '{{ var("data_final_veiculo_arquitetura_1") }}'
            or data_fim_lacre is null
        union all by name
        select *
        from lacre_atualizado
    )

select *
from dados_finais
where
    not (  -- veículos com placa corrigida
        (
            id_veiculo in ("D17039", "D17037")
            and data_fim_lacre is null
            and data_inicio_lacre = "2025-10-14"
        )
        -- Correção de duplicação de lacre por alteração no id_veiculo
        or (
            id_veiculo = "B11579"
            and data_fim_lacre is null
            and data_inicio_lacre = "2025-12-09"
        )
    )
