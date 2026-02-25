{{
    config(
        alias="lancamento",
    )
}}

select
    data,
    hora,
    id_lancamento,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    safe_cast(json_value(content, '$.id_conta') as string) as id_conta,
    safe_cast(json_value(content, '$.vl_lancamento') as numeric) as vl_lancamento,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.dt_lancamento') as string)
        ),
        "America/Sao_Paulo"
    ) as dt_lancamento,
    safe_cast(json_value(content, '$.id_movimento') as string) as id_movimento,
    safe_cast(json_value(content, '$.id_lote_credito') as string) as id_lote_credito,
    safe_cast(
        json_value(content, '$.cd_tipo_movimento') as string
    ) as cd_tipo_movimento,
    safe_cast(
        json_value(content, '$.ds_tipo_movimento') as string
    ) as ds_tipo_movimento,
    safe_cast(json_value(content, '$.ds_tipo_conta') as string) as ds_tipo_conta,
    safe_cast(json_value(content, '$.id_tipo_moeda') as string) as id_tipo_moeda,
    safe_cast(json_value(content, '$.tipo_moeda') as string) as tipo_moeda,
    regexp_replace(
        safe_cast(json_value(content, '$.cd_cliente') as string), r'\.0$', ''
    ) as cd_cliente,
    safe_cast(json_value(content, '$.nr_logico_midia') as string) as nr_logico_midia
from {{ source("source_jae", "lancamento") }}
