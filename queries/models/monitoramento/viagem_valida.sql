{% if var("tipo_materializacao") == "monitoramento" %}
    {{
        config(
            partition_by={
                "field": "data",
                "data_type": "date",
                "granularity": "day",
            },
            schema="monitoramento_interno",
        )
    }}
{% else %}
    {{
        config(
            partition_by={
                "field": "data",
                "data_type": "date",
                "granularity": "day",
            },
        )
    }}
{% endif %}

{% set viagem_validacao = ref("viagem_validacao") %}
{% if execute and is_incremental() %}
    {% set partitions = get_modified_partitions_filter(
        viagem_validacao,
        truncate_date=true,
        max_age_days=var("viagem_validacao_max_age_days", 5),
    ) %}
{% else %} {% set partitions = [] %}
{% endif %}

{% set incremental_filter %}
    {% if is_incremental() %}
        {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
        {% else %} data = date("2000-01-01")
        {% endif %}
    {% else %}
        data between date('{{ var("date_range_start") }}') and date(
            '{{ var("date_range_end") }}'
        )
    {% endif %}
    and data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
{% endset %}

with
    veiculo as (
        select data, id_veiculo, placa, ano_fabricacao, tecnologia, status
        from {{ ref("aux_veiculo_dia_consolidada") }}
        where
            data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
            {% if is_incremental() %} and {{ incremental_filter }} {% endif %}
    ),
    viagem_valida as (
        select *
        from {{ viagem_validacao }}
        {# from `rj-smtr.monitoramento.viagem_validacao` v #}
        where
            indicador_viagem_valida
            and data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
            {% if is_incremental() %} and {{ incremental_filter }} {% endif %}
    )
select
    vv.data,
    vv.id_viagem,
    vv.id_viagem_planejada,
    vv.datetime_partida_considerada as datetime_partida,
    vv.datetime_chegada_considerada as datetime_chegada,
    vv.modo,
    vv.consorcio,
    vv.sistema,
    vv.vista,
    vv.tipo_dia,
    vv.servico,
    vv.route_id,
    vv.trip_id,
    vv.shape_id,
    vv.sentido,
    vv.id_veiculo,
    ve.placa,
    ve.ano_fabricacao,
    ve.tecnologia as tecnologia_apurada,
    ve.status as tipo_viagem,
    vv.feed_start_date,
    vv.distancia_planejada,
    vv.velocidade_media,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ var("version") }}' as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from viagem_valida as vv
left join veiculo as ve using (data, id_veiculo)
