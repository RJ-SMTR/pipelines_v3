with
    -- 1. Lista os data-serviço-tipo serviços corrigidos
    correcao_raw as (
        select
            data,
            servico,
            servico_corrigido,
            tipo,
            if(
                tipo = 'Sem planejamento porém com receita tarifária', 1, 2
            ) as prioridade
        from {{ ref("staging_encontro_contas_correcao_servico_rdo") }}
        where servico_corrigido is not null and servico != servico_corrigido
        qualify
            row_number() over (
                partition by data, servico, tipo order by data_resposta desc
            )
            = 1
    ),
    -- 2. Remove os data-serviço-tipo serviços com referências circulares, priorizando
    -- os do tipo "Subsídio pago sem receita tarifária"
    pares_circulares as (
        select
            data,
            least(servico, servico_corrigido) as chave_a,
            greatest(servico, servico_corrigido) as chave_b,
            servico,
            servico_corrigido,
            tipo,
            prioridade
        from correcao_raw
        qualify
            row_number() over (partition by data, chave_a, chave_b order by prioridade)
            = 1
    )

select
    data,
    servico,
    servico_corrigido,
    tipo,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from pares_circulares
