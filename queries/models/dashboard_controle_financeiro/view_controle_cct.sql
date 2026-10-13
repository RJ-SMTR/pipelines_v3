{{
    config(
        materialized="view",
    )
}}

select
    data,
    tipo,
    operacao,
    favorecido,
    if(tipo_conta = 'CAER', 'N/A', modal) as modal,
    valor,
    ano_competencia,
    mes_competencia,
    if(
        tipo_conta = 'CAER', 'N/A', quinzena_semana_competencia
    ) as quinzena_semana_competencia,
    numero_processo,
    observacoes,
    if(tipo_conta = 'CETT', 'N/A', numero_nf) as numero_nf,
    tipo_conta
from {{ ref("controle_cct") }}
