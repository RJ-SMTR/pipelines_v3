{{
    config(
        alias="fiscalizacao",
    )
}}

select
    data,
    hora,
    id,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    safe_cast(json_value(content, '$.cd_conta_acesso') as string) as cd_conta_acesso,
    replace(
        safe_cast(json_value(content, '$.id_tipo_fiscalizacao') as string), ".0", ""
    ) as id_tipo_fiscalizacao,
    safe_cast(json_value(content, '$.nr_cpf') as string) as nr_cpf,
    safe_cast(json_value(content, '$.pan_hash') as string) as pan_hash,
    replace(
        safe_cast(json_value(content, '$.id_operadora') as string), ".0", ""
    ) as id_operadora,
    replace(
        safe_cast(json_value(content, '$.id_linha') as string), ".0", ""
    ) as id_linha,
    safe_cast(json_value(content, '$.in_geracao_multa') as bool) as in_geracao_multa,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.dt_inclusao') as string)
        ),
        "America/Sao_Paulo"
    ) as dt_inclusao,
    safe_cast(json_value(content, '$.latitude') as float64) as latitude,
    safe_cast(json_value(content, '$.longitude') as float64) as longitude,
    replace(
        safe_cast(json_value(content, '$.login_atendente') as string), ".0", ""
    ) as login_atendente,
    safe_cast(
        json_value(content, '$.tx_retorno_bp_server') as string
    ) as tx_retorno_bp_server,
    replace(
        safe_cast(json_value(content, '$.id_linha_fiscalizada') as string), ".0", ""
    ) as id_linha_fiscalizada,
    safe_cast(
        json_value(content, '$.descricao_tipo_fiscalizacao') as string
    ) as descricao_tipo_fiscalizacao
from {{ source("source_jae", "fiscalizacao") }}
