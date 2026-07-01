{{ config(materialized="view") }}

select
    data,
    safe_cast(codrec as string) as id_receita,
    safe_cast(anodarm as int64) as ano_darm,
    safe_cast(darm as string) as darm,
    safe_cast(emissao as string) as emissao,
    safe_cast(json_value(content, '$.banco') as string) as banco,
    safe_cast(json_value(content, '$.valor_pgto') as numeric) as valor_pagamento,
    safe_cast(json_value(content, '$.data_pgto') as date) as data_pagamento,
    safe_cast(json_value(content, '$.data_apro') as date) as data_aprovacao,
    safe_cast(
        json_value(content, '$.vl_pgto_acum') as numeric
    ) as valor_pagamento_acumulado,
    safe_cast(
        json_value(content, '$.GerarZapCarioca') as boolean
    ) as indicador_gerar_zap_carioca,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S%Ez',
            safe_cast(json_value(content, '$._datetime_execucao_flow') as string)
        ),
        "America/Sao_Paulo"
    ) as datetime_execucao_flow,
    safe_cast(timestamp_captura as datetime) as datetime_captura
from {{ source("source_stu", "darm_apropriacao") }}
