{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  Flags de bilhetagem (sem transação / validador) a partir do efêmero
  compartilhado. Sem prioridade de tipo_viagem.
#}
{% set incremental_filter %}
    data between date("{{ var('date_range_start') }}") and date("{{ var('date_range_end') }}")
{% endset %}

select
    data,
    id_viagem,
    id_veiculo,
    datetime_partida,
    datetime_chegada,
    modo,
    tecnologia_apurada,
    tecnologia_remunerada,
    indicador_sem_transacao,
    indicador_sem_transacao_tipo,
    indicador_validador_fechado,
    indicador_validador_associado_incorretamente,
    indicador_estado_equipamento_aberto,
    quantidade_transacao,
    quantidade_transacao_riocard,
    percentual_estado_equipamento_aberto,
    datetime_partida_bilhetagem,
    id_validador,
    json_set(
        json_set(
            json_set(
                json_set(
                    indicadores,
                    '$.indicador_sem_transacao.valor',
                    coalesce(indicador_sem_transacao, false)
                ),
                '$.indicador_sem_transacao_tipo.valor',
                coalesce(indicador_sem_transacao_tipo, false)
            ),
            '$.indicador_validador_fechado.valor',
            coalesce(indicador_validador_fechado, false)
        ),
        '$.indicador_validador_associado_incorretamente.valor',
        coalesce(indicador_validador_associado_incorretamente, false)
    ) as indicadores,
    servico,
    sentido,
    distancia_planejada,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("eph_viagem_transacao") }}
where {{ incremental_filter }}
