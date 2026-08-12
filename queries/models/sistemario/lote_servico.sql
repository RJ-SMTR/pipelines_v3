{{
    config(
        materialized="table",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  Associação serviço → lote a partir das tabelas do Anexo I.2 (POR por
  serviço — seed `sistema_referencia_servico`, ex. p.18+).

  Vigência por data_inicio/data_fim (fases entrada/plena/plena_expandida
  do I.2 §4.1 materializadas como datas no seed — sem coluna rede).
  Relacionados: `operacao_lote` / `operacao_lote_tecnologia` (Tab.24–26).
#}
with
    base as (
        select
            cast(servico as string) as servico,
            cast(lote as string) as lote,
            cast(servico_tipo as string) as servico_tipo,
            cast(servico_tecnologia as string) as servico_tecnologia,
            cast(tipo_dia as string) as tipo_dia,
            cast(data_inicio as date) as data_inicio,
            cast(nullif(data_fim, "") as date) as data_fim
        from {{ ref("sistema_referencia_servico") }}
    ),
    -- Um lote por serviço × vigência (tipo_dia pode variar; prioriza util).
    priorizado as (
        select
            servico,
            lote,
            servico_tipo,
            servico_tecnologia,
            data_inicio,
            data_fim,
            row_number() over (
                partition by servico, data_inicio, data_fim
                order by
                    case tipo_dia when "util" then 1 when "sabado" then 2 else 3 end,
                    lote
            ) as rn
        from base
    )
select
    servico,
    lote,
    servico_tipo,
    servico_tecnologia,
    data_inicio,
    data_fim,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from priorizado
where rn = 1
