{{ config(materialized="ephemeral") }}

select
    t.data,
    t.id_viagem,
    t.id_veiculo,
    t.servico,
    t.id_validador,
    {{
        tipo_viagem_sppo(
            "t.data",
            "t.tipo_viagem",
            "coalesce(rt.indicador_detectado_ar_inoperante, false)",
            sem_transacao="t.indicador_sem_transacao",
            sem_transacao_tipo="coalesce(t.indicador_sem_transacao_tipo, false)",
            estado_equipamento_aberto="t.indicador_estado_equipamento_aberto",
            validador_fechado="coalesce(t.indicador_validador_fechado, false)",
            validador_associado_incorretamente="coalesce(t.indicador_validador_associado_incorretamente, false)",
        )
    }}
    as tipo_viagem,
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
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from {{ ref("aux_viagem_transacao") }} as t
left join
    {{ ref("aux_viagem_regularidade_temperatura") }} as rt
    on t.data = rt.data
    and t.id_viagem = rt.id_viagem
where t.sistema = "SPPO"
