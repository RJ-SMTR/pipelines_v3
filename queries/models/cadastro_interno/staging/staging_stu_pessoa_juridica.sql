{{ config(materialized="view") }}

select
    data,
    safe_cast(cgc as string) as cgc,
    safe_cast(json_value(content, '$.cod_estado') as string) as sigla_uf,
    safe_cast(json_value(content, '$.nomefant') as string) as nome_fantasia,
    safe_cast(json_value(content, '$.razao_social') as string) as razao_social,
    safe_cast(json_value(content, '$.endereco') as string) as endereco,
    safe_cast(json_value(content, '$.numero') as int64) as numero,
    safe_cast(json_value(content, '$.complemento') as string) as complemento,
    safe_cast(json_value(content, '$.bairro') as string) as bairro,
    safe_cast(json_value(content, '$.municipio') as string) as municipio,
    safe_cast(json_value(content, '$.cep') as string) as cep,
    safe_cast(
        json_value(content, '$.inscr_municipal') as string
    ) as inscricao_municipal,
    safe_cast(json_value(content, '$.banco') as string) as banco,
    safe_cast(json_value(content, '$.agencia') as string) as agencia,
    safe_cast(json_value(content, '$.conta') as string) as conta,
    safe_cast(json_value(content, '$.situac') as string) as situacao,
    safe_cast(
        json_value(content, '$.erro_migracao') as boolean
    ) as indicador_erro_migracao,
    safe_cast(json_value(content, '$.letra_consorcio') as string) as letra_consorcio,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S%Ez',
            safe_cast(json_value(content, '$._datetime_execucao_flow') as string)
        ),
        "America/Sao_Paulo"
    ) as datetime_execucao_flow,
    safe_cast(timestamp_captura as datetime) as datetime_captura
from {{ source("source_stu", "pessoa_juridica") }}
