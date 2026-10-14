with
    -- Transações Jaé
    transacao as (
        select
             t.id_veiculo,
            
            t.servico_jae,
            t.datetime_transacao
        from `rj-smtr`.`bilhetagem`.`transacao` as t
        -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao`
        where
            t.data between date(
  
       "2026-08-02T00:00:00"
    
) and date_add(
                date(
  
       "2026-08-02T00:00:00"
    
), interval 1 day
            )
            and date(t.datetime_processamento) - date(t.datetime_transacao)
            <= interval 6 day
            and t.modo = "Ônibus"
    ),

    -- Transações RioCard
    transacao_riocard as (
        select
             t.id_veiculo,
            
            t.servico_jae,
            t.datetime_transacao
        from `rj-smtr`.`bilhetagem`.`transacao_riocard` as t
        -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao_riocard`
        where
            t.data between date(
  
       "2026-08-02T00:00:00"
    
) and date_add(
                date(
  
       "2026-08-02T00:00:00"
    
), interval 1 day
            )
            and date(t.datetime_processamento) - date(t.datetime_transacao)
            <= interval 6 day
            and t.modo = "Ônibus"
    ),

    -- -- Viagens realizadas
    viagem as (
        select
            data,
            id_viagem,
            id_veiculo,
             substr(id_veiculo, 2) as id_veiculo_join,
            
            cast(null as string) as id_validador,
            datetime_partida,
            datetime_chegada,
            modo,
            tecnologia_apurada,
            tecnologia_remunerada,
             tipo_viagem,
            
            indicadores,
            servico,
            sentido,
            distancia_planejada
         from `rj-smtr`.`subsidio`.`viagem_regularidade_temperatura`
        
        where
            data >= date("2025-07-16")
            and (
                data between date(
  
       "2026-08-02T00:00:00"
    
) and date(
  
       "2026-08-02T00:00:00"
    
)
                
                    or data = date_sub(date(
  
       "2026-08-02T00:00:00"
    
), interval 1 day)
                
            )

        
    ),

    -- Viagem, para fins de contagem de passageiros, com tolerância de 30 minutos,
    -- limitada pela viagem anterior
    viagem_com_tolerancia_previa as (
        select
            v.*,
            lag(v.datetime_chegada) over (
                partition by v.id_veiculo order by v.datetime_partida
            ) as viagem_anterior_chegada,
            case
                when
                    lag(v.datetime_chegada) over (
                        partition by v.id_veiculo order by v.datetime_partida
                    )
                    is null
                then
                    datetime(
                        timestamp_sub(
                            datetime_partida, interval 30 minute
                        )
                    )
                else
                    datetime(
                        timestamp_add(
                            greatest(
                                timestamp_sub(
                                    datetime_partida,
                                    interval 30 minute
                                ),
                                lag(v.datetime_chegada) over (
                                    partition by v.id_veiculo
                                    order by v.datetime_partida
                                )
                            ),
                            interval 1 second
                        )
                    )
            end as datetime_partida_com_tolerancia
        from viagem as v
    ),

    -- Considera apenas as viagens realizadas no período de apuração
    viagem_com_tolerancia as (
        select *
        from viagem_com_tolerancia_previa
        where data between date(
  
       "2026-08-02T00:00:00"
    
) and date(
  
       "2026-08-02T00:00:00"
    
)
    ),

    -- Contagem de transações Jaé
    transacao_contagem as (
        select
            v.data,
            v.id_viagem,
            count(t.datetime_transacao) as quantidade_transacao,
            countif(
                v.servico != t.servico_jae and t.datetime_transacao > v.datetime_partida
            ) as quantidade_transacao_servico_divergente
        from transacao as t
        inner join
            viagem_com_tolerancia as v
            on t.id_veiculo = v.id_veiculo_join
            and t.datetime_transacao
            between v.datetime_partida_com_tolerancia and v.datetime_chegada
        group by 1, 2
    ),

    -- Contagem de transações RioCard
    transacao_riocard_contagem as (
        select
            v.data,
            v.id_viagem,
            count(tr.datetime_transacao) as quantidade_transacao_riocard,
            countif(
                v.servico != tr.servico_jae
                and tr.datetime_transacao > v.datetime_partida
            ) as quantidade_transacao_riocard_servico_divergente
        from transacao_riocard as tr
        inner join
            viagem_com_tolerancia as v
            on tr.id_veiculo = v.id_veiculo_join
            and tr.datetime_transacao
            between v.datetime_partida_com_tolerancia and v.datetime_chegada
        group by 1, 2
    ),

    -- Calcula a porcentagem de estado do equipamento "ABERTO" por
    -- validador e
    -- viagem
    estado_equipamento_perc as (
        select
            v.data,
            v.id_viagem,
            coalesce(
                safe_cast(json_value(item, "$.id_validador") as string), v.id_validador
            ) as id_validador,
            coalesce(t.quantidade_transacao, 0) as quantidade_transacao,
            coalesce(
                tr.quantidade_transacao_riocard, 0
            ) as quantidade_transacao_riocard,
            coalesce(
                t.quantidade_transacao_servico_divergente, 0
            ) as quantidade_transacao_servico_divergente,
            coalesce(
                tr.quantidade_transacao_riocard_servico_divergente, 0
            ) as quantidade_transacao_riocard_servico_divergente,
            safe_cast(
                json_value(item, '$.percentual_estado_equipamento_aberto') as numeric
            ) as percentual_estado_equipamento_aberto,
            safe_cast(
                json_value(item, '$.indicador_estado_equipamento_aberto') as bool
            ) as indicador_estado_equipamento_aberto,
            safe_cast(
                json_value(item, '$.indicador_gps_servico_divergente') as bool
            ) as indicador_gps_servico_divergente
        from viagem as v
        left join
            transacao_contagem as t on v.data = t.data and v.id_viagem = t.id_viagem
        left join
            transacao_riocard_contagem as tr
            on v.data = tr.data
            and v.id_viagem = tr.id_viagem
        left join
            unnest(
                json_query_array(v.indicadores, '$.indicador_validador.valores')
            ) as item
    ),

    validador_tipo_viagem as (
        select
            data,
            id_viagem,
            id_validador,
            case
                when data < date('2024-11-16')
                then quantidade_transacao_riocard = 0
                else (quantidade_transacao_riocard = 0 and quantidade_transacao = 0)
            end as indicador_sem_transacao,
            indicador_estado_equipamento_aberto,
            (
                data >= date('2024-07-20')
                and (
                    (
                        data < date('2024-11-16')
                        and (
                            quantidade_transacao_riocard = 0
                            or not indicador_estado_equipamento_aberto
                        )
                    )
                    or (
                        data >= date('2024-11-16')
                        and data < date('3000-01-01')
                        and (
                            (
                                quantidade_transacao_riocard = 0
                                and quantidade_transacao = 0
                            )
                            or not indicador_estado_equipamento_aberto
                        )
                    )
                    or (
                        data >= date('3000-01-01')
                        and (
                            quantidade_transacao_riocard = 0
                            and quantidade_transacao = 0
                        )
                    )
                )
            ) as indicador_sem_transacao_tipo,
            (
                data >= date('3000-01-01')
                and not indicador_estado_equipamento_aberto
            ) as indicador_validador_fechado,
            (
                data >= date('3000-01-01')
                and (
                    quantidade_transacao_riocard_servico_divergente > 0
                    or quantidade_transacao_servico_divergente > 0
                    or indicador_gps_servico_divergente
                )
            ) as indicador_validador_associado_incorretamente
        from estado_equipamento_perc
    ),

    flags_viagem as (
        select
            data,
            id_viagem,
            max(indicador_sem_transacao) as indicador_sem_transacao,
            max(indicador_sem_transacao_tipo) as indicador_sem_transacao_tipo,
            logical_or(indicador_validador_fechado) as indicador_validador_fechado,
            logical_or(
                indicador_validador_associado_incorretamente
            ) as indicador_validador_associado_incorretamente,
            case
                when data < date('3000-01-01')
                then max(indicador_estado_equipamento_aberto)
                else min(indicador_estado_equipamento_aberto)
            end as indicador_estado_equipamento_aberto,
            array_agg(distinct id_validador ignore nulls) as id_validador
        from validador_tipo_viagem
        group by 1, 2
    )

select
    v.data,
    v.id_viagem,
    v.id_veiculo,
    v.servico,
    any_value(f.id_validador) as id_validador,
    v.tipo_viagem,
    f.indicador_sem_transacao,
    f.indicador_sem_transacao_tipo,
    f.indicador_validador_fechado,
    f.indicador_validador_associado_incorretamente,
    v.modo,
    v.tecnologia_apurada,
    v.tecnologia_remunerada,
    v.sentido,
    v.distancia_planejada,
    any_value(v.indicadores) as indicadores,
    any_value(eep.quantidade_transacao) as quantidade_transacao,
    any_value(eep.quantidade_transacao_riocard) as quantidade_transacao_riocard,
    case
        when v.data < date('3000-01-01')
        then max(eep.percentual_estado_equipamento_aberto)
        else min(eep.percentual_estado_equipamento_aberto)
    end as percentual_estado_equipamento_aberto,
    f.indicador_estado_equipamento_aberto,
    v.datetime_partida_com_tolerancia as datetime_partida_bilhetagem,
    v.datetime_partida,
    v.datetime_chegada
from viagem_com_tolerancia as v
left join
    estado_equipamento_perc as eep on v.data = eep.data and v.id_viagem = eep.id_viagem
left join flags_viagem as f on v.data = f.data and v.id_viagem = f.id_viagem
group by all