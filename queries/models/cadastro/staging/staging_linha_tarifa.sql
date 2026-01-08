{{
    config(
        alias="linha_tarifa",
    )
}}

with
    linha_tarifa as (
        select
            data,
            safe_cast(cd_linha as string) as cd_linha,
            safe_cast(nr_sequencia as integer) as nr_sequencia,
            datetime(
                parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
                "America/Sao_Paulo"
            ) as timestamp_captura,
            safe_cast(
                json_value(content, '$.vl_tarifa_ida') as numeric
            ) as vl_tarifa_ida,
            safe_cast(
                json_value(content, '$.vl_tarifa_volta') as numeric
            ) as vl_tarifa_volta,
            safe_cast(json_value(content, '$.vl_pedagio') as numeric) as vl_pedagio,
            safe_cast(json_value(content, '$.vl_taxas') as numeric) as vl_taxas,
            safe_cast(
                json_value(content, '$.cd_tipo_cobranca') as string
            ) as cd_tipo_cobranca,
            safe_cast(
                json_value(content, '$.cd_tipo_valor') as string
            ) as cd_tipo_valor,
            safe_cast(
                json_value(content, '$.cd_dia_semana') as string
            ) as cd_dia_semana,
            safe_cast(json_value(content, '$.cd_mes') as string) as cd_mes,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E6S%Ez',
                    safe_cast(json_value(content, '$.dt_inclusao') as string)
                ),
                "America/Sao_Paulo"
            ) as dt_inclusao,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%E6S%Ez',
                    safe_cast(json_value(content, '$.dt_inicio_validade') as string)
                ),
                "America/Sao_Paulo"
            ) as dt_inicio_validade
        from
            {{
                source(
                    "source_jae",
                    "linha_tarifa",
                )
            }}
    ),
    linha_tarifa_rn as (
        select
            *,
            row_number() over (
                partition by cd_linha, nr_sequencia order by timestamp_captura desc
            ) as rn
        from linha_tarifa
    )
select * except (rn)
from linha_tarifa_rn
where rn = 1
