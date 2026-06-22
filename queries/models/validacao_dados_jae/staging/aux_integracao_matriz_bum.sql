with
    matriz_integracao as (
        select *
        from {{ ref("matriz_integracao") }}
        where tipo_bilhete_unico = 'BUM' and tipo_integracao != "Transferência"
    ),

    integracao_sem_transferencia as (
        select *
        from {{ ref("aux_integracao_invalida_origem_destino") }}
        where
            (
                not 'VLT' in unnest(modos_origem)
                or not exists (
                    select 1
                    from unnest(modos_origem) o
                    join unnest(modos_destino) d on o = d
                )
            )
    )
select
    i.*,
    m.integracao is null as indicador_integracao_fora_matriz,
    m.tempo_integracao_minutos,
    case
        when rn = 1  -- a primeira transação com origem e destino
        then
            [
                struct(
                    percentual_rateio_origem as percentual_rateio,
                    i.modos_origem_string as modo,
                    1 as ordem
                ),
                struct(
                    percentual_rateio_destino as percentual_rateio,
                    i.modos_destino_string as modo,
                    2 as ordem
                )
            ]
        else  -- transações seguintes somente destino (a origem é igual ao destino da transação anterior)
            [
                struct(
                    percentual_rateio_destino as percentual_rateio,
                    i.modos_destino_string as modo,
                    1 as ordem
                )
            ]
    end as array_integracao
from integracao_sem_transferencia i
left join
    matriz_integracao m
    on i.data_lead >= m.data_inicio
    and (i.data_lead <= m.data_fim or m.data_fim is null)
    and (
        i.id_servico_jae_origem = m.id_servico_jae_origem
        or m.id_servico_jae_origem is null
    )
    and ifnull(m.modo_destino, '') in unnest(i.modos_destino)
    and (
        (
            ifnull(m.modo_origem, '') in unnest(i.modos_origem)
            and i.integracao_origem_regex is null
        )
        or regexp_extract(m.integracao_origem, i.integracao_origem_regex) is not null
    )
