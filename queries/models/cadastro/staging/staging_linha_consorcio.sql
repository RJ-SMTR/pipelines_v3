{{
    config(
        alias="linha_consorcio",
    )
}}

with
    linha_consorcio as (
        select
            data,
            safe_cast(cd_consorcio as string) as cd_consorcio,
            safe_cast(cd_linha as string) as cd_linha,
            timestamp_captura,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%S%Ez',
                    safe_cast(json_value(content, '$.DT_INCLUSAO') as string)
                ),
                "America/Sao_Paulo"
            ) as dt_inclusao,
            parse_date(
                "%Y-%m-%d",
                safe_cast(json_value(content, '$.DT_INICIO_VALIDADE') as string)
            ) as dt_inicio_validade,
            parse_date(
                "%Y-%m-%d",
                safe_cast(json_value(content, '$.DT_FIM_VALIDADE') as string)
            ) as dt_fim_validade
        from {{ source("source_jae", "linha_consorcio") }}
    ),
    linha_consorcio_rn as (
        select
            *,
            row_number() over (
                partition by cd_consorcio, cd_linha order by timestamp_captura desc
            ) as rn
        from linha_consorcio
    )
select * except (rn)
from linha_consorcio_rn
where rn = 1
