{{
    config(
        alias="cett",
    )
}}

with
    content_data as (
        select
            concat("[", content, "]") as json_content,
            timestamp_captura,
            date(data) as data
        from {{ source("controle_financeiro_staging", "cett") }}

        union all

        select
            concat("[", content, "]") as json_content,
            timestamp_captura,
            date(data) as data
        from {{ source("source_smtr", "cett") }}
    )
select
    data,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    parse_date(
        '%d/%m/%Y', safe_cast(json_value(content, '$.Data') as string)
    ) as data_operacao,
    safe_cast(json_value(content, '$.Tipo') as string) as tipo,
    safe_cast(json_value(content, '$.Operação') as string) as operacao,
    safe_cast(json_value(content, '$.Favorecido') as string) as favorecido,
    safe_cast(json_value(content, '$.Modal') as string) as modal,
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
    safe_cast(json_value(content, '$.Ano Competência') as integer) as ano_competencia,
    safe_cast(json_value(content, '$.Mês Competência') as integer) as mes_competencia,
    safe_cast(
        json_value(content, '$.Quinzena Competência') as string
    ) as quinzena_competencia,
    safe_cast(json_value(content, '$.Nº Processo') as string) as numero_processo,
    safe_cast(json_value(content, '$.Observações') as string) as observacoes,
    safe_cast(json_value(content, '$.Pendências') as string) as pendencias,
    safe_cast(json_value(content, '$.Ano Transação') as integer) as ano_transacao,
    safe_cast(json_value(content, '$.Mês Transação') as integer) as mes_transacao,
    safe_cast(
        replace(
            replace(
                replace(
                    json_value(content, '$.Valor a ser pago para o STUC (Relatório)'),
                    'R$ ',
                    ''
                ),
                '.',
                ''
            ),
            ',',
            '.'
        ) as float64
    ) as valor_stuc,
    safe_cast(
        replace(
            replace(
                replace(json_value(content, '$.Pagamentos pendente (STUC)'), 'R$ ', ''),
                '.',
                ''
            ),
            ',',
            '.'
        ) as float64
    ) as valor_pendente_stuc,
    safe_cast(
        replace(
            replace(
                replace(
                    json_value(
                        content, '$.Saldo total dos pagamentos pendentes (STUC)'
                    ),
                    'R$ ',
                    ''
                ),
                '.',
                ''
            ),
            ',',
            '.'
        ) as float64
    ) as saldo_total_pagamento_pendente_stuc
from content_data, unnest(json_extract_array(json_content)) as content
