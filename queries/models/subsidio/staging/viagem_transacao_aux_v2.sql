{{ config(materialized="ephemeral") }}

select
    data,
    id_viagem,
    id_veiculo,
    servico,
    id_validador,
    case
        when
            tipo_viagem not in (
                "Licenciado com ar e não autuado", "Licenciado sem ar e não autuado"
            )
        then tipo_viagem
        when indicador_sem_transacao_tipo
        then
            case
                when
                    data < date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
                    and not indicador_sem_transacao
                    and indicador_estado_equipamento_aberto
                then tipo_viagem
                else "Sem transação"
            end
        when indicador_validador_fechado
        then "Validador fechado"
        when indicador_validador_associado_incorretamente
        then "Validador associado incorretamente"
        else tipo_viagem
    end as tipo_viagem,
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
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from {{ ref("eph_viagem_transacao") }}
