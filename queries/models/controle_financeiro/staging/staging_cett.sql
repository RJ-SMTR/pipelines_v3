{{
    config(
        alias="cett",
    )
}}

with
    content_data as (
        select concat("[", content, "]") as json_content, timestamp_captura
        from {{ source("controle_financeiro_staging", "cett") }}

        union all

        select concat("[", content, "]") as json_content, timestamp_captura
        from {{ source("source_smtr", "cett") }}
    )
select
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
    timestamp(timestamp_captura) as timestamp_captura
from content_data, unnest(json_extract_array(json_content)) as content
