{{ config(materialized="ephemeral") }}

{% set incremental_filter %}
    data between date("{{ var('date_range_start') }}") and date("{{ var('date_range_end') }}") and data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset %}

{#
  SPPO: lê aux_viagem_temperatura (tabela do monitoramento) para a apuração
  não inline eph_viagem_temperatura.
  RIO: não há aux persistido; o flow passa sistema=rio.
#}
{% set condicao_veiculo %}
(
    (
        vt.ano_fabricacao <= 2019
        or vt.data >= date('{{ var("DATA_SUBSIDIO_V19_INICIO") }}')
    )
    and (
        not vt.indicador_temperatura_nula_viagem
        or (
            vt.data >= date('{{ var("DATA_SUBSIDIO_V22_INICIO") }}')
            and coalesce(vr.indicador_falha_recorrente, false)
        )
    )
)
{% endset %}

with
    fonte as (  -- Normaliza indicadores para JSON independente do sistema de origem
        select
            data,
            id_viagem,
            id_veiculo,
            id_validador,
            datetime_partida,
            datetime_chegada,
            modo,
            ano_fabricacao,
            tecnologia_apurada,
            tecnologia_remunerada,
            tipo_viagem,
            servico,
            sentido,
            distancia_planejada,
            {% if var("sistema") == "rio" %} parse_json(indicadores_str) as indicadores
            {% else %} indicadores
            {% endif %}
        {% if var("sistema") == "rio" %} from {{ ref("eph_viagem_temperatura") }}
        {% else %} from {{ ref("aux_viagem_temperatura") }}
        {% endif %}
        where {{ incremental_filter }}
    ),
    viagem_temperatura as (
        select
            data,
            id_viagem,
            id_veiculo,
            id_validador,
            datetime_partida,
            datetime_chegada,
            modo,
            ano_fabricacao,
            tecnologia_apurada,
            tecnologia_remunerada,
            tipo_viagem,
            servico,
            sentido,
            distancia_planejada,
            indicadores,
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
                    '$.indicador_temperatura_pos_tratamento_descartada_viagem.valor'
                ) as bool
            ) as indicador_temperatura_pos_tratamento_descartada_viagem,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_zero_viagem.valor'
                ) as bool
            ) as indicador_temperatura_zero_viagem,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_nula_viagem.valor'
                ) as bool
            ) as indicador_temperatura_nula_viagem,
            safe_cast(
                json_value(
                    indicadores, '$.indicador_temperatura_regular_viagem.valor'
                ) as bool
            ) as indicador_temperatura_regular_viagem
        from fonte
    ),
    veiculo_regularidade as (
        select
            data,
            id_veiculo,
            indicadores.indicador_falha_recorrente.valor as indicador_falha_recorrente,
            indicadores.indicador_falha_recorrente.data_verificacao_falha
            as data_verificacao_falha
        from {{ ref("veiculo_regularidade_temperatura_dia") }}
        where {{ incremental_filter }}
    )
select
    vt.data,
    vt.id_viagem,
    vt.id_veiculo,
    vt.id_validador,
    vt.datetime_partida,
    vt.datetime_chegada,
    vt.modo,
    vt.ano_fabricacao,
    vt.tecnologia_apurada,
    vt.tecnologia_remunerada,
    vt.tipo_viagem,
    {{ condicao_veiculo }}
    and (
        (
            vt.data >= date('{{ var("DATA_SUBSIDIO_V20_INICIO") }}')
            and coalesce(vr.indicador_falha_recorrente, false)
        )
        or vt.indicador_temperatura_zero_viagem
        or not vt.indicador_temperatura_transmitida_viagem
        or not vt.indicador_temperatura_regular_viagem
    ) as indicador_detectado_ar_inoperante,
    case
        when {{ condicao_veiculo }}
        then
            (
                (
                    vt.data < date('{{ var("DATA_SUBSIDIO_V20_INICIO") }}')
                    or (
                        vt.data >= date('{{ var("DATA_SUBSIDIO_V20_INICIO") }}')
                        and not coalesce(vr.indicador_falha_recorrente, false)
                    )
                )
                and not vt.indicador_temperatura_zero_viagem
                and vt.indicador_temperatura_transmitida_viagem
                and vt.indicador_temperatura_regular_viagem
            )
        when vt.indicador_temperatura_nula_viagem = true
        then true
    end as indicador_regularidade_ar_condicionado_viagem,
    vr.indicador_falha_recorrente,
    vr.data_verificacao_falha,
    vt.indicadores,
    vt.servico,
    vt.sentido,
    vt.distancia_planejada
from viagem_temperatura as vt
left join
    veiculo_regularidade as vr on vt.data = vr.data and vt.id_veiculo = vr.id_veiculo
