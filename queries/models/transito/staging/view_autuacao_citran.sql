{{ config(materialized="view") }}


select distinct
    date(data) as data,
    parse_date(
        '%d/%m/%Y', safe_cast(json_value(content, '$.Data') as string)
    ) data_autuacao,
    safe_cast(json_value(content, '$.Hora') as string) hora,
    -- fmt: off
    Cod__Detran as id_auto_infracao,
    -- fmt: on
    if(
        json_value(content, '$.DtLimDP') != '',
        parse_date('%d/%m/%Y', json_value(content, '$.DtLimDP')),
        null

    ) as data_limite_defesa_previa,
    if(
        json_value(content, '$.DtLimR') != '',
        parse_date('%d/%m/%Y', json_value(content, '$.DtLimR')),
        null
    ) as data_limite_recurso,
    safe_cast(json_value(content, '$.Situacao Atual') as string) situacao_atual,
    safe_cast(json_value(content, '$."St. Infracao"') as string) status_infracao,
    safe_cast(json_value(content, '$.Multa') as string) codigo_enquadramento,
    safe_cast(json_value(content, '$.DsInf') as string) tipificacao_resumida,
    safe_cast(json_value(content, '$.Po') as string) pontuacao,
    if(
        json_value(content, '$.Tipo') != '',
        safe_cast(json_value(content, '$.Tipo') as string),
        null
    ) as tipo_veiculo,
    safe_cast(json_value(content, '$.Marca') as string) descricao_veiculo,
    safe_cast(json_value(content, '$.Esp') as string) especie_veiculo,
    safe_cast(json_value(content, '$.CDUF') as string) uf_proprietario,
    safe_cast(json_value(content, '$.Cep') as string) cep_proprietario,
    safe_cast(json_value(content, '$.Ufir') as numeric) valor_infracao,
    safe_cast(json_value(content, '$.VlPagto') as numeric) valor_pago,
    if(
        json_value(content, '$.DtPagto') != '',
        parse_date('%d/%m/%Y', json_value(content, '$.DtPagto')),
        null
    ) data_pagamento,
    safe_cast(json_value(content, '$.Orgao') as string) descricao_autuador,
    safe_cast(json_value(content, '$.LocInf') as string) endereco_autuacao,
    safe_cast(json_value(content, '$.ProAutu') as string) processo_defesa_autuacao,
    safe_cast(json_value(content, '$.NotifPen') as string) recurso_penalidade_multa,
    safe_cast(json_value(content, '$.ProcRI') as string) processo_troca_real_infrator,
from {{ source("transito_staging", "autuacao_citran") }}
