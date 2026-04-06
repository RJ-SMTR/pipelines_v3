{{
    config(
        alias="produto",
    )
}}

with
    produto as (
        select
            data,
            datetime(
                parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
                "America/Sao_Paulo"
            ) as timestamp_captura,
            replace(safe_cast(cd_produto as string), '.0', '') as cd_produto,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%S%Ez',
                    safe_cast(json_value(content, '$.DT_INCLUSAO') as string)
                ),
                "America/Sao_Paulo"
            ) as datetime_inclusao,
            safe_cast(
                json_value(content, '$.CD_EMISSOR_PRODUTO') as string
            ) as cd_emissor_produto,
            safe_cast(json_value(content, '$.NM_PRODUTO') as string) as nm_produto,
            safe_cast(
                json_value(content, '$.TX_DESCRITIVO_PRODUTO') as string
            ) as tx_descritivo_produto
        from {{ source("source_jae", "produto") }}
    ),
    produto_rn as (
        select
            *,
            row_number() over (
                partition by cd_produto order by timestamp_captura desc
            ) as rn
        from produto
    )
select * except (rn)
from produto_rn
where rn = 1
