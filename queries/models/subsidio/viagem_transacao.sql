{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

select
    data,
    id_viagem,
    id_veiculo,
    servico,
    id_validador,
    tipo_viagem,
    modo,
    tecnologia_apurada,
    tecnologia_remunerada,
    sentido,
    distancia_planejada,
    quantidade_transacao,
    quantidade_transacao_riocard,
    percentual_estado_equipamento_aberto,
    indicador_estado_equipamento_aberto,
    datetime_partida_bilhetagem,
    datetime_partida,
    datetime_chegada,
    datetime_ultima_atualizacao
from {{ ref("viagem_transacao_aux_v2") }}
where data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}") full outer
union all by name

select
    data,
    id_viagem,
    id_veiculo,
    servico,
    id_validador,
    tipo_viagem,
    modo,
    tecnologia_apurada,
    tecnologia_remunerada,
    sentido,
    distancia_planejada,
    quantidade_transacao,
    quantidade_transacao_riocard,
    percentual_estado_equipamento_aberto,
    indicador_estado_equipamento_aberto,
    datetime_partida_bilhetagem,
    datetime_partida,
    datetime_chegada,
    datetime_ultima_atualizacao
from {{ ref("viagem_transacao_aux_v1") }}
where data < date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
