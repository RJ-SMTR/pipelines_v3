{{
    config(
        alias="caer",
    )
}}

select
    data as data_captura,
    timestamp(timestamp_captura) as timestamp_captura,
    parse_date('%d/%m/%Y', safe_cast(json_value(content, '$.Data') as string)) as data,
    safe_cast(json_value(content, '$.Lançamento') as string) as lancamento,
    safe_cast(json_value(content, '$.Operação') as string) as operacao,
    safe_cast(json_value(content, '$.Tipo') as string) as tipo,
    safe_cast(
        replace(
            replace(replace(json_value(content, '$.Valor'), 'R$ ', ''), '.', ''),
            ',',
            '.'
        ) as float64
    ) as valor,
    safe_cast(
        replace(
            replace(replace(json_value(content, '$.Saldo Final'), 'R$ ', ''), '.', ''),
            ',',
            '.'
        ) as float64
    ) as saldo_final,
    safe_cast(json_value(content, '$.Favorecido') as string) as favorecido,
    safe_cast(json_value(content, '$.Modal') as string) as modal,
    safe_cast(json_value(content, '$.Pendências') as string) as pendencias,
    safe_cast(json_value(content, '$.Observações') as string) as observacoes,
    safe_cast(json_value(content, '$.Ano') as integer) as ano,
    safe_cast(json_value(content, '$.Mês') as integer) as mes,
    safe_cast(json_value(content, '$.Ano Referência') as integer) as ano_referencia,
    safe_cast(json_value(content, '$.Mês Referência') as integer) as mes_referencia,
    safe_cast(json_value(content, '$.Nº Processo') as string) as numero_processo,
    safe_cast(json_value(content, '$.Nº NF') as string) as numero_nf
from {{ source("source_smtr", "caer") }}
