{{ config(materialized="ephemeral") }}

with
    status as (select * from {{ ref("aux_viagem_status") }}),

    temperatura as (
        select
            data,
            id_viagem,
            indicador_detectado_ar_inoperante,
            indicador_regularidade_ar_condicionado_viagem
        from {{ ref("aux_viagem_regularidade_temperatura") }}
    ),

    bilhetagem as (
        select
            data,
            id_viagem,
            indicador_sem_transacao,
            indicador_sem_transacao_tipo,
            indicador_validador_fechado,
            indicador_validador_associado_incorretamente,
            indicador_estado_equipamento_aberto
        from {{ ref("aux_viagem_transacao") }}
    )

select
    s.data,
    s.id_viagem,
    s.datetime_partida,
    s.datetime_chegada,
    s.tipo_execucao_viagem,
    s.id_veiculo,
    s.placa,
    s.ano_fabricacao,
    s.servico,
    s.sentido,
    s.shape_id,
    s.distancia_planejada,
    s.modo,
    s.tipo_dia,
    s.tipo_dia_oferta,
    s.sistema,
    s.tipo_viagem,
    s.tecnologia_apurada,
    s.tecnologia_remunerada,
    s.menor_tecnologia_permitida,
    s.maior_tecnologia_permitida,
    s.lote,
    s.faixa_horaria_inicio,
    s.faixa_horaria_fim,
    s.indicador_viagem_apurada,
    s.indicadores,
    b.indicador_estado_equipamento_aberto,
    cast(s.distancia_planejada as float64) as km_programada,
    cast(
        if(
            coalesce(s.tipo_execucao_viagem, "COMPLETA") = "INCOMPLETA",
            least(
                coalesce(s.km_percorrida_gps, 0),
                coalesce(cast(s.distancia_planejada as float64), 0)
            ),
            cast(s.distancia_planejada as float64)
        ) as float64
    ) as km_percorrida,
    coalesce(s.indicador_nao_licenciado, false) as indicador_nao_licenciado,
    coalesce(s.indicador_nao_vistoriado, false) as indicador_nao_vistoriado,
    coalesce(s.indicador_lacrado, false) as indicador_lacrado,
    coalesce(
        s.indicador_nao_autorizado_capacidade, false
    ) as indicador_nao_autorizado_capacidade,
    coalesce(
        s.indicador_autuado_ar_inoperante, false
    ) as indicador_autuado_ar_inoperante,
    coalesce(
        s.indicador_autuado_alterar_itinerario, false
    ) as indicador_autuado_alterar_itinerario,
    coalesce(
        s.indicador_autuado_vista_inoperante, false
    ) as indicador_autuado_vista_inoperante,
    coalesce(
        s.indicador_autuado_nao_atender_parada, false
    ) as indicador_autuado_nao_atender_parada,
    coalesce(
        s.indicador_autuado_nao_concluir_itinerario, false
    ) as indicador_autuado_nao_concluir_itinerario,
    coalesce(
        s.indicador_registrado_ar_inoperante, false
    ) as indicador_registrado_ar_inoperante,
    coalesce(
        t.indicador_detectado_ar_inoperante, false
    ) as indicador_detectado_ar_inoperante,
    coalesce(
        t.indicador_regularidade_ar_condicionado_viagem, true
    ) as indicador_regularidade_ar_condicionado_viagem,
    coalesce(b.indicador_sem_transacao, false) as indicador_sem_transacao,
    coalesce(b.indicador_sem_transacao_tipo, false) as indicador_sem_transacao_tipo,
    coalesce(b.indicador_validador_fechado, false) as indicador_validador_fechado,
    coalesce(
        b.indicador_validador_associado_incorretamente, false
    ) as indicador_validador_associado_incorretamente
from status as s
left join temperatura as t using (data, id_viagem)
left join bilhetagem as b using (data, id_viagem)
