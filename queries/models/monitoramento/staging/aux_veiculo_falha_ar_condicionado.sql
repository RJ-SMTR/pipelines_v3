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

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
    and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset %}

{% set condicao_falha %}
    (not indicador_temperatura_variacao_veiculo
    or not indicador_temperatura_transmitida_veiculo
    or indicador_temperatura_descartada_veiculo
    or indicador_viagem_temperatura_descartada_veiculo)
{% endset %}

with
    viagem_temperatura as (
        select
            data,
            id_viagem,
            id_veiculo,
            placa,
            ano_fabricacao,
            quantidade_pre_tratamento,
            quantidade_pos_tratamento,
            safe_cast(
                json_value(indicadores, '$.indicador_ar_condicionado.valor') as bool
            ) as indicador_ar_condicionado,
            safe_cast(
                json_value(
                    indicadores,
                    '$.indicador_ar_condicionado.data_processamento_licenciamento'
                ) as date
            ) as data_processamento_licenciamento,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_variacao_viagem.valor'
                ) as bool
            ) as indicador_temperatura_variacao_viagem,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_transmitida_viagem.valor'
                ) as bool
            ) as indicador_temperatura_transmitida_viagem,
            safe_cast(
                json_value(
                    indicadores,
                    '$.indicador_temperatura_pos_tratamento_descartada_viagem.percentual_temperatura_pos_tratamento_descartada'
                ) as numeric
            ) as percentual_temperatura_pos_tratamento_descartada_viagem,
            safe_cast(
                json_value(
                    indicadores,
                    '$.indicador_temperatura_pos_tratamento_descartada_viagem.datetime_verificacao_regularidade'
                ) as datetime
            ) as datetime_verificacao_regularidade,
        from {{ ref("aux_viagem_temperatura") }}
        where
            {{ incremental_filter }}
            and (
                ano_fabricacao <= 2019
                or data >= date('{{ var("DATA_SUBSIDIO_V19_INICIO") }}')
            )
    ),
    agg_viagem_temperatura as (
        select
            data,
            id_veiculo,
            placa,
            ano_fabricacao,
            indicador_ar_condicionado,
            data_processamento_licenciamento,
            max(
                indicador_temperatura_variacao_viagem
            ) as indicador_temperatura_variacao_veiculo,
            max(
                indicador_temperatura_transmitida_viagem
            ) as indicador_temperatura_transmitida_veiculo,
            trunc(
                (
                    1 - coalesce(
                        safe_divide(
                            sum(quantidade_pos_tratamento),
                            sum(quantidade_pre_tratamento)
                        ),
                        0
                    )
                )
                * 100,
                2
            ) as percentual_temperatura_pos_tratamento_descartada,
            trunc(
                coalesce(
                    safe_divide(
                        countif(
                            percentual_temperatura_pos_tratamento_descartada_viagem > 50
                        ),
                        count(id_viagem)
                    ),
                    0
                )
                * 100,
                2
            ) as percentual_viagem_temperatura_pos_tratamento_descartada,
            max(
                date(datetime_verificacao_regularidade)
            ) as data_verificacao_regularidade
        from viagem_temperatura
        where indicador_ar_condicionado
        group by all
    ),
    indicador_veiculo as (
        select distinct
            data,
            id_veiculo,
            placa,
            ano_fabricacao,
            indicador_ar_condicionado,
            data_processamento_licenciamento,
            data_verificacao_regularidade,
            indicador_temperatura_variacao_veiculo,
            indicador_temperatura_transmitida_veiculo,
            percentual_temperatura_pos_tratamento_descartada,
            percentual_temperatura_pos_tratamento_descartada
            > 50 as indicador_temperatura_descartada_veiculo,
            percentual_viagem_temperatura_pos_tratamento_descartada,
            percentual_viagem_temperatura_pos_tratamento_descartada
            > 50 as indicador_viagem_temperatura_descartada_veiculo
        from agg_viagem_temperatura
    ),
    {% if is_incremental() %}
        dia_anterior as (
            select id_veiculo, quantidade_dia_falha_operacional
            from {{ this }}
            where data = date_sub(date("{{ var('date_range_start') }}"), interval 1 day)
        ),
        veiculos as (
            select distinct id_veiculo
            from viagem_temperatura
            union distinct
            select distinct id_veiculo
            from dia_anterior
        ),
    {% else %} veiculos as (select distinct id_veiculo from viagem_temperatura),
    {% endif %}
    datas as (
        select data
        from
            unnest(
                generate_date_array(
                    date("{{var('date_range_start')}}"),
                    date("{{var('date_range_end')}}")
                )
            ) as data
    ),
    datas_veiculos as (select data, id_veiculo from datas cross join veiculos),
    falha_dia as (
        select
            d.*,
            i.* except (data, id_veiculo),
            last_value(placa ignore nulls) over (
                partition by id_veiculo
                order by data
                rows between unbounded preceding and 1 preceding
            ) as placa_anterior,
            case
                when {{ condicao_falha }}
                then true
                when not {{ condicao_falha }}
                then false
                else null
            end as indicio_falha
        from datas_veiculos as d
        left join indicador_veiculo as i using (data, id_veiculo)
    ),
    falha_consecutiva as (
        select
            *,
            countif(indicio_falha) over (
                partition by id_veiculo, grupo order by data
            ) as quantidade_dia_falha_operacional
        from
            (
                select
                    *,
                    countif(not indicio_falha or placa != placa_anterior) over (
                        partition by id_veiculo
                        order by data
                        rows between unbounded preceding and current row
                    ) as grupo
                from falha_dia
            )
    ),
    {% if is_incremental() %}
        falha_consecutiva_completa as (
            select
                d.data,
                d.id_veiculo,
                f.* except (data, id_veiculo, grupo, quantidade_dia_falha_operacional),
                case
                    when grupo = 0
                    then
                        f.quantidade_dia_falha_operacional
                        + coalesce(da.quantidade_dia_falha_operacional, 0)
                    else f.quantidade_dia_falha_operacional
                end as quantidade_dia_falha_operacional
            from datas_veiculos as d
            left join falha_consecutiva as f using (data, id_veiculo)
            left join dia_anterior as da using (id_veiculo)
        ),
    {% else %}
        falha_consecutiva_completa as (
            select d.data, d.id_veiculo, f.* except (data, id_veiculo, grupo)
            from datas_veiculos as d
            left join falha_consecutiva as f using (data, id_veiculo)
        ),
    {% endif %}
    final as (
        select
            * except (quantidade_dia_falha_operacional, placa_anterior),
            last_value(quantidade_dia_falha_operacional ignore nulls) over (
                partition by id_veiculo
                order by data
                rows between unbounded preceding and current row
            ) as quantidade_dia_falha_operacional,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
            "{{ var('version') }}" as versao,
            '{{ invocation_id }}' as id_execucao_dbt
        from falha_consecutiva_completa
    )
select *
from final
