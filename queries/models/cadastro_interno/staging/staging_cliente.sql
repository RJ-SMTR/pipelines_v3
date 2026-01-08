{{ config(alias="cliente", tags=["identificacao"]) }}


select
    data,
    replace(safe_cast(cd_cliente as string), '.0', '') as cd_cliente,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    replace(
        safe_cast(json_value(content, '$.CD_TIPO_DOCUMENTO') as string), ".0", ""
    ) as cd_tipo_documento,
    safe_cast(json_value(content, '$.NM_CLIENTE') as string) as nm_cliente,
    safe_cast(
        json_value(content, '$.NM_CLIENTE_SOCIAL') as string
    ) as nm_cliente_social,
    safe_cast(
        json_value(content, '$.IN_TIPO_PESSOA_FISICA_JURIDICA') as string
    ) as in_tipo_pessoa_fisica_juridica,
    safe_cast(json_value(content, '$.NR_DOCUMENTO') as string) as nr_documento,
    safe_cast(
        json_value(content, '$.NR_DOCUMENTO_ALTERNATIVO') as string
    ) as nr_documento_alternativo,
    safe_cast(json_value(content, '$.TX_EMAIL') as string) as tx_email,
    safe_cast(json_value(content, '$.NR_TELEFONE') as string) as nr_telefone,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.DT_CADASTRO') as string)
        ),
        "America/Sao_Paulo"
    ) as dt_cadastro
from {{ source("source_jae", "cliente") }}
