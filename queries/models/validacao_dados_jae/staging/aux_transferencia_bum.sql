with
    matriz_integracao as (
        select *
        from {{ ref("matriz_integracao") }}
        where tipo_bilhete_unico = 'BUM' and tipo_integracao = "Transferência"
    ),
    transferencia as (
        /*
        Filtra somente as transferências
        */
        select
            *,
            row_number() over (
                partition by id_integracao order by sequencia_integracao
            ) as rn
        from {{ ref("aux_integracao_invalida_origem_destino") }}
        where
            'VLT' in unnest(modos_origem)
            and exists (
                select 1
                from unnest(modos_origem) o
                join unnest(modos_destino) d on o = d
            )
    ),
    mudanca_transferencia as (
        /*
        Classifica as transferências em agrupamentos
        */
        select
            *,
            case
                when
                    sequencia_integracao = lag(sequencia_integracao) over (
                        partition by id_integracao order by rn
                    )
                    + 1
                then 0
                else 1
            end as indicador_mudanca_transferencia
        from transferencia
    ),
    transferencia_id as (
        /*
        Identifica as transferências dentro de uma integração
        */
        select
            sum(indicador_mudanca_transferencia) over (
                partition by id_integracao order by rn
            ) as id_transferencia,
            *
        from mudanca_transferencia
    )
/*
1. Faz o join com a matriz
2. Cria um array com informações da transferência para ser tratado posteriormente
*/
select
    t.*,
    m.integracao is null as indicador_transferencia_fora_matriz,
    m.tempo_integracao_minutos,
    case
        when rn = 1  -- a primeira transação com origem e destino
        then
            [
                struct(t.modos_origem_string as modo, 1 as ordem),
                struct(t.modos_destino_string as modo, 2 as ordem)
            ]
        else [struct(t.modos_destino_string as modo, 1 as ordem)]  -- transações seguintes somente destino (a origem é igual ao destino da transação anterior)
    end as array_modo
from transferencia_id t
left join
    matriz_integracao m
    on t.data_lead >= m.data_inicio
    and (t.data_lead <= m.data_fim or m.data_fim is null)
    and m.modo_origem in unnest(i.modos_origem)
    and (
        t.id_servico_jae_origem = m.id_servico_jae_origem
        or m.id_servico_jae_origem is null
    )
    and m.modo_destino in unnest(i.modos_destino)
    and (
        t.id_servico_jae_destino = m.id_servico_jae_destino
        or m.id_servico_jae_destino is null
    )
