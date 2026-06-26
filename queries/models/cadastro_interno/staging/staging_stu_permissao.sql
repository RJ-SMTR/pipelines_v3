{{ config(alias="permissao") }}

select
    data,
    tptran,
    tpperm,
    termo,
    dv,
    safe_cast(json_value(content, '$.proaut') as string) as processo_autorizacao,
    date(safe_cast(json_value(content, '$.dtauto') as string)) as data_autorizacao,
    safe_cast(json_value(content, '$.procanc') as string) as processo_cancelamento,
    date(safe_cast(json_value(content, '$.dtcanc') as string)) as data_cancelamento,
    safe_cast(json_value(content, '$.situac') as string) as situacao,
    safe_cast(json_value(content, '$.motivo') as string) as motivo,
    safe_cast(json_value(content, '$.sorteio') as boolean) as sorteio,
    safe_cast(json_value(content, '$.dnm') as boolean) as dnm,
    safe_cast(json_value(content, '$.origem') as string) as origem,
    safe_cast(json_value(content, '$.intransferivel') as boolean) as intransferivel,
    safe_cast(json_value(content, '$.sem_auxiliar') as boolean) as sem_auxiliar,
    safe_cast(json_value(content, '$.id_artigo') as string) as id_artigo,
    safe_cast(json_value(content, '$.codigoHash') as string) as codigo_hash,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S%Ez',
            safe_cast(json_value(content, '$._datetime_execucao_flow') as string)
        ),
        "America/Sao_Paulo"
    ) as datetime_execucao_flow,
    timestamp_captura
from {{ source("source_stu", "permissao") }}
