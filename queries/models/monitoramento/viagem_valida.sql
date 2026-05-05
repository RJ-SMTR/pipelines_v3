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
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

with
    veiculo as (
        select data, id_veiculo, placa, ano_fabricacao, tecnologia, status
        from {{ ref("aux_veiculo_dia_consolidada") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    viagem_valida as (
        select *
        from {{ ref("viagem_validacao") }}
        {# from `rj-smtr.monitoramento.viagem_validacao` v #}
        where
            indicador_viagem_valida
            {% if is_incremental() %} and {{ incremental_filter }} {% endif %}
    )
select
    vv.data,
    vv.id_viagem,
    vv.id_viagem_planejada,
    vv.id_veiculo,
    ve.placa,
    ve.ano_fabricacao,
    vv.datetime_partida_considerada as datetime_partida,
    vv.datetime_chegada_considerada as datetime_chegada,
    vv.modo,
    ve.tecnologia as tecnologia_apurada,
    ve.status as tipo_viagem,
    vv.servico,
    vv.sentido,
    vv.distancia_planejada,
    vv.feed_start_date,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from viagem_valida as vv
left join veiculo as ve using (data, id_veiculo)
