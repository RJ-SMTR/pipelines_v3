{{
    config(
        alias="linha_sem_ressarcimento",
    )
}}

with
    linha_sem_ressarcimento as (
        select
            data,
            safe_cast(id_linha as string) as id_linha,
            datetime(
                parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
                "America/Sao_Paulo"
            ) as timestamp_captura,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E*S%Ez',
                    safe_cast(json_value(content, '$.dt_inclusao') as string)
                ),
                'America/Sao_Paulo'
            ) as dt_inclusao
        from {{ source("source_jae", "linha_sem_ressarcimento") }}
    )
select * except (rn)
from
    (
        select
            *,
            row_number() over (
                partition by id_linha order by timestamp_captura desc
            ) as rn
        from linha_sem_ressarcimento
    )
where rn = 1
