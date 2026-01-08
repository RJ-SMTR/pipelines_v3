{{
    config(
        alias="endereco",
    )
}}


select
    data,
    replace(safe_cast(nr_seq_endereco as string), '.0', '') as nr_seq_endereco,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    replace(
        safe_cast(json_value(content, '$.CD_CLIENTE') as string), '.0', ''
    ) as cd_cliente,
    replace(
        safe_cast(json_value(content, '$.CD_TIPO_ENDERECO') as string), '.0', ''
    ) as cd_tipo_endereco,
    replace(
        safe_cast(json_value(content, '$.CD_TIPO_LOGRADOURO') as string), '.0', ''
    ) as cd_tipo_logradouro,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%S%Ez',
            safe_cast(json_value(content, '$.DT_INCLUSAO') as string)
        ),
        "America/Sao_Paulo"
    ) as dt_inclusao,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%S%Ez',
            safe_cast(json_value(content, '$.DT_INATIVACAO') as string)
        ),
        "America/Sao_Paulo"
    ) as dt_inativacao,
    safe_cast(json_value(content, '$.NM_BAIRRO') as string) as nm_bairro,
    safe_cast(json_value(content, '$.NM_CIDADE') as string) as nm_cidade,
    safe_cast(json_value(content, '$.NR_CEP') as string) as nr_cep,
    safe_cast(json_value(content, '$.NR_LOGRADOURO') as string) as nr_logradouro,
    safe_cast(json_value(content, '$.SG_UF') as string) as sg_uf,
    safe_cast(
        json_value(content, '$.TX_COMPLEMENTO_LOGRADOURO') as string
    ) as tx_complemento_logradouro,
    safe_cast(json_value(content, '$.TX_LOGRADOURO') as string) as tx_logradouro
from {{ source("source_jae", "endereco") }}
