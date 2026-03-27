{{
    config(
        alias="ordem_rateio",
    )
}}

with
    ordem_rateio as (
        select
            data,
            replace(safe_cast(id as string), '.0', '') as id_ordem_rateio,
            timestamp_captura,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E*S%Ez',
                    safe_cast(json_value(content, '$.data_inclusao') as string)
                ),
                'America/Sao_Paulo'
            ) as data_inclusao,
            parse_date(
                '%Y-%m-%d', safe_cast(json_value(content, '$.data_ordem') as string)
            ) as data_ordem,
            replace(
                safe_cast(json_value(content, '$.id_consorcio') as string), '.0', ''
            ) as id_consorcio,
            replace(
                safe_cast(json_value(content, '$.id_linha') as string), '.0', ''
            ) as id_linha,
            replace(
                safe_cast(json_value(content, '$.id_operadora') as string), '.0', ''
            ) as id_operadora,
            replace(
                safe_cast(json_value(content, '$.id_ordem_pagamento') as string),
                '.0',
                ''
            ) as id_ordem_pagamento,
            replace(
                safe_cast(
                    json_value(content, '$.id_ordem_pagamento_consorcio') as string
                ),
                '.0',
                ''
            ) as id_ordem_pagamento_consorcio,
            replace(
                safe_cast(
                    json_value(
                        content, '$.id_ordem_pagamento_consorcio_operadora'
                    ) as string
                ),
                '.0',
                ''
            ) as id_ordem_pagamento_consorcio_operadora,
            safe_cast(
                json_value(content, '$.id_status_ordem') as string
            ) as id_status_ordem,
            safe_cast(
                safe_cast(
                    json_value(
                        content, '$.qtd_rateio_compensacao_credito_t0'
                    ) as float64
                ) as integer
            ) as qtd_rateio_compensacao_credito_t0,
            safe_cast(
                safe_cast(
                    json_value(
                        content, '$.qtd_rateio_compensacao_credito_t1'
                    ) as float64
                ) as integer
            ) as qtd_rateio_compensacao_credito_t1,
            safe_cast(
                safe_cast(
                    json_value(
                        content, '$.qtd_rateio_compensacao_credito_t2'
                    ) as float64
                ) as integer
            ) as qtd_rateio_compensacao_credito_t2,
            safe_cast(
                safe_cast(
                    json_value(
                        content, '$.qtd_rateio_compensacao_credito_t3'
                    ) as float64
                ) as integer
            ) as qtd_rateio_compensacao_credito_t3,
            safe_cast(
                safe_cast(
                    json_value(
                        content, '$.qtd_rateio_compensacao_credito_t4'
                    ) as float64
                ) as integer
            ) as qtd_rateio_compensacao_credito_t4,
            safe_cast(
                safe_cast(
                    json_value(
                        content, '$.qtd_rateio_compensacao_credito_total'
                    ) as float64
                ) as integer
            ) as qtd_rateio_compensacao_credito_total,
            safe_cast(
                safe_cast(
                    json_value(content, '$.qtd_rateio_compensacao_debito_t0') as float64
                ) as integer
            ) as qtd_rateio_compensacao_debito_t0,
            safe_cast(
                safe_cast(
                    json_value(content, '$.qtd_rateio_compensacao_debito_t1') as float64
                ) as integer
            ) as qtd_rateio_compensacao_debito_t1,
            safe_cast(
                safe_cast(
                    json_value(content, '$.qtd_rateio_compensacao_debito_t2') as float64
                ) as integer
            ) as qtd_rateio_compensacao_debito_t2,
            safe_cast(
                safe_cast(
                    json_value(content, '$.qtd_rateio_compensacao_debito_t3') as float64
                ) as integer
            ) as qtd_rateio_compensacao_debito_t3,
            safe_cast(
                safe_cast(
                    json_value(content, '$.qtd_rateio_compensacao_debito_t4') as float64
                ) as integer
            ) as qtd_rateio_compensacao_debito_t4,
            safe_cast(
                safe_cast(
                    json_value(
                        content, '$.qtd_rateio_compensacao_debito_total'
                    ) as float64
                ) as integer
            ) as qtd_rateio_compensacao_debito_total,
            safe_cast(
                json_value(content, '$.valor_rateio_compensacao_credito_t0') as numeric
            ) as valor_rateio_compensacao_credito_t0,
            safe_cast(
                json_value(content, '$.valor_rateio_compensacao_credito_t1') as numeric
            ) as valor_rateio_compensacao_credito_t1,
            safe_cast(
                json_value(content, '$.valor_rateio_compensacao_credito_t2') as numeric
            ) as valor_rateio_compensacao_credito_t2,
            safe_cast(
                json_value(content, '$.valor_rateio_compensacao_credito_t3') as numeric
            ) as valor_rateio_compensacao_credito_t3,
            safe_cast(
                json_value(content, '$.valor_rateio_compensacao_credito_t4') as numeric
            ) as valor_rateio_compensacao_credito_t4,
            safe_cast(
                json_value(
                    content, '$.valor_rateio_compensacao_credito_total'
                ) as numeric
            ) as valor_rateio_compensacao_credito_total,
            safe_cast(
                json_value(content, '$.valor_rateio_compensacao_debito_t0') as numeric
            ) as valor_rateio_compensacao_debito_t0,
            safe_cast(
                json_value(content, '$.valor_rateio_compensacao_debito_t1') as numeric
            ) as valor_rateio_compensacao_debito_t1,
            safe_cast(
                json_value(content, '$.valor_rateio_compensacao_debito_t2') as numeric
            ) as valor_rateio_compensacao_debito_t2,
            safe_cast(
                json_value(content, '$.valor_rateio_compensacao_debito_t3') as numeric
            ) as valor_rateio_compensacao_debito_t3,
            safe_cast(
                json_value(content, '$.valor_rateio_compensacao_debito_t4') as numeric
            ) as valor_rateio_compensacao_debito_t4,
            safe_cast(
                json_value(
                    content, '$.valor_rateio_compensacao_debito_total'
                ) as numeric
            ) as valor_rateio_compensacao_debito_total
        from {{ source("source_jae", "ordem_rateio") }}
    )
select * except (rn)
from
    (
        select
            *,
            row_number() over (
                partition by id_ordem_rateio order by timestamp_captura
            ) as rn
        from ordem_rateio
    )
where rn = 1
