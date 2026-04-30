{{
    config(
        alias="operadora_transporte",
    )
}}

select
    data,
    replace(
        safe_cast(cd_operadora_transporte as string), ".0", ""
    ) as cd_operadora_transporte,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%S%Ez',
            safe_cast(json_value(content, '$.DT_INCLUSAO') as string)
        ),
        "America/Sao_Paulo"
    ) as datetime_inclusao,
    replace(
        safe_cast(json_value(content, '$.CD_CLIENTE') as string), ".0", ""
    ) as cd_cliente,
    replace(
        safe_cast(json_value(content, '$.CD_TIPO_CLIENTE') as string), ".0", ""
    ) as cd_tipo_cliente,
    replace(
        safe_cast(json_value(content, '$.CD_TIPO_MODAL') as string), ".0", ""
    ) as cd_tipo_modal,
    safe_cast(
        json_value(content, '$.IN_SITUACAO_ATIVIDADE') as string
    ) as in_situacao_atividade,
    safe_cast(json_value(content, '$.DS_TIPO_MODAL') as string) as ds_tipo_modal
from {{ source("source_jae", "operadora_transporte") }}
