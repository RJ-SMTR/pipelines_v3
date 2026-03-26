{{
    config(
        alias="ordem_pagamento",
    )
}}

with
    ordem_pagamento as (
        select
            data,
            replace(safe_cast(id as string), '.0', '') as id_ordem_pagamento,
            timestamp_captura,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E*S%Ez',
                    safe_cast(json_value(content, '$.data_inclusao') as string)
                ),
                "America/Sao_Paulo"
            ) as datetime_inclusao,
            parse_date(
                '%Y-%m-%d', safe_cast(json_value(content, '$.data_ordem') as string)
            ) as data_ordem,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E*S%Ez',
                    safe_cast(json_value(content, '$.data_pagamento') as string)
                ),
                "America/Sao_Paulo"
            ) as data_pagamento,
            safe_cast(
                json_value(content, '$.id_status_ordem') as string
            ) as id_status_ordem,
            safe_cast(json_value(content, '$.qtd_debito') as integer) as qtd_debito,
            safe_cast(
                json_value(content, '$.qtd_gratuidade') as integer
            ) as qtd_gratuidade,
            safe_cast(
                json_value(content, '$.qtd_integracao') as integer
            ) as qtd_integracao,
            safe_cast(
                json_value(content, '$.qtd_rateio_credito') as integer
            ) as qtd_rateio_credito,
            safe_cast(
                json_value(content, '$.qtd_rateio_debito') as integer
            ) as qtd_rateio_debito,
            safe_cast(
                json_value(content, '$.qtd_vendaabordo') as integer
            ) as qtd_vendaabordo,
            safe_cast(json_value(content, '$.valor_bruto') as numeric) as valor_bruto,
            safe_cast(json_value(content, '$.valor_debito') as numeric) as valor_debito,
            safe_cast(
                json_value(content, '$.valor_gratuidade') as numeric
            ) as valor_gratuidade,
            safe_cast(
                json_value(content, '$.valor_integracao') as numeric
            ) as valor_integracao,
            safe_cast(
                json_value(content, '$.valor_liquido') as numeric
            ) as valor_liquido,
            safe_cast(
                json_value(content, '$.valor_rateio_credito') as numeric
            ) as valor_rateio_credito,
            safe_cast(
                json_value(content, '$.valor_rateio_debito') as numeric
            ) as valor_rateio_debito,
            safe_cast(json_value(content, '$.valor_taxa') as numeric) as valor_taxa,
            safe_cast(
                json_value(content, '$.valor_vendaabordo') as numeric
            ) as valor_vendaabordo
        from {{ source("source_jae", "ordem_pagamento") }}
    ),
    ordem_pagamento_rn as (
        select
            *,
            row_number() over (
                partition by id_ordem_pagamento order by timestamp_captura
            ) as rn
        from ordem_pagamento
    )
select * except (rn)
from ordem_pagamento_rn
where rn = 1
