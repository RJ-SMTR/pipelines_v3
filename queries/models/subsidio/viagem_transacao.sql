{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

select
    t.data,
    t.id_viagem,
    t.id_veiculo,
    t.servico,
    t.id_validador,
    vc.tipo_viagem,
    t.modo,
    t.tecnologia_apurada,
    t.tecnologia_remunerada,
    t.sentido,
    t.distancia_planejada,
    t.quantidade_transacao,
    t.quantidade_transacao_riocard,
    t.percentual_estado_equipamento_aberto,
    t.indicador_estado_equipamento_aberto,
    t.datetime_partida_bilhetagem,
    t.datetime_partida,
    t.datetime_chegada,
    t.datetime_ultima_atualizacao
from {{ ref("viagem_transacao_aux_v2") }} as t
left join {{ ref("viagem_classificada") }} as vc using (data, id_viagem)
where t.data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
full outer union all by name

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
