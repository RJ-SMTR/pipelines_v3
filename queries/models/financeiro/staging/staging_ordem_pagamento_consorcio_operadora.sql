{{
    config(
        alias="ordem_pagamento_consorcio_operadora",
    )
}}

with
    ordem_pagamento_consorcio_operadora as (
        select
            data,
            safe_cast(id as string) as id_ordem_pagamento_consorcio_operadora,
            datetime(
                parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
                "America/Sao_Paulo"
            ) as timestamp_captura,
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
            replace(
                safe_cast(json_value(content, '$.id_consorcio') as string), ".0", ""
            ) as id_consorcio,
            replace(
                safe_cast(json_value(content, '$.id_operadora') as string), ".0", ""
            ) as id_operadora,
            replace(
                safe_cast(
                    json_value(content, '$.id_ordem_pagamento_consorcio') as string
                ),
                ".0",
                ""
            ) as id_ordem_pagamento_consorcio,
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
        from
            {{
                source(
                    "source_jae",
                    "ordem_pagamento_consorcio_operadora",
                )
            }}
    ),
    ordem_pagamento_consorcio_operadora_rn as (
        select
            *,
            row_number() over (
                partition by id_ordem_pagamento_consorcio_operadora
                order by timestamp_captura
            ) as rn
        from ordem_pagamento_consorcio_operadora
    )
select * except (rn)
from ordem_pagamento_consorcio_operadora_rn
where rn = 1
