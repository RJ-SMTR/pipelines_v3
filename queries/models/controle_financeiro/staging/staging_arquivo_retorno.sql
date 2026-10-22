{{ config(alias="arquivo_retorno") }}

select
    data,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    id,
    date(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E3SZ',
            safe_cast(json_value(content, '$.dataCaptura') as string)
        )
    ) as datacaptura,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E3SZ',
            safe_cast(json_value(content, '$.dataHoraGeracaoRetorno') as string)
        ),
        "America/Sao_Paulo"
    ) as datahorageracaoretorno,
    date(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E3SZ',
            safe_cast(json_value(content, '$.dataOrdem') as string)
        )
    ) as dataordem,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E3SZ',
            safe_cast(json_value(content, '$.dataProcessamento') as string)
        ),
        "America/Sao_Paulo"
    ) as dataprocessamento,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E3S%Ez',
            safe_cast(json_value(content, '$.dataVencimento') as string)
        )
    ) as datavencimento,
    safe_cast(json_value(content, '$.favorecido') as string) as favorecido,
    safe_cast(json_value(content, '$.idConsorcio') as string) as idconsorcio,
    safe_cast(json_value(content, '$.idOperadora') as string) as idoperadora,
    safe_cast(json_value(content, '$.idOrdemPagamento') as string) as idordempagamento,
    safe_cast(json_value(content, '$.isPago') as bool) as ispago,
    safe_cast(json_value(content, '$.nomeConsorcio') as string) as nomeconsorcio,
    safe_cast(json_value(content, '$.nomeOperadora') as string) as nomeoperadora,
    json_value(content, '$.ocorrencias') as ocorrencias,
    safe_cast(json_value(content, '$.valor') as numeric) as valor,
    safe_cast(
        json_value(content, '$.valorRealEfetivado') as numeric
    ) as valorrealefetivado
from {{ source("controle_financeiro_staging", "arquivo_retorno") }}
