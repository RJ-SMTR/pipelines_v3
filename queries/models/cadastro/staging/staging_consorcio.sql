{{
    config(
        alias="consorcio",
    )
}}

with
    consorcio as (
        select
            data,
            replace(safe_cast(cd_consorcio as string), '.0', '') as cd_consorcio,
            timestamp_captura,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%S%Ez',
                    safe_cast(json_value(content, '$.DT_INCLUSAO') as string)
                ),
                "America/Sao_Paulo"
            ) as datetime_inclusao,
            safe_cast(json_value(content, '$.NM_CONSORCIO') as string) as nm_consorcio
        from {{ source("source_jae", "consorcio") }}
    ),
    consorcio_rn as (
        select
            *,
            row_number() over (
                partition by cd_consorcio order by timestamp_captura desc
            ) as rn
        from consorcio
    )
select * except (rn)
from consorcio_rn
where rn = 1
