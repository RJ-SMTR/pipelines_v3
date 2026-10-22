{{
    config(
        alias="viagem_informada_conecta_sistema_rio",
    )
}}

select
    data,
    safe_cast(id_viagem as string) as id_viagem,
    safe_cast(json_value(content, '$.sequencial_viagem') as int64) as sequencial_viagem,
    safe_cast(
        json_value(content, '$.id_viagem_planejada') as string
    ) as id_viagem_planejada,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as datetime_captura,
    datetime(
        safe_cast(json_value(content, '$.datetime_chegada') as timestamp),
        'America/Sao_Paulo'
    ) as datetime_chegada,
    datetime(
        safe_cast(json_value(content, '$.datetime_partida') as timestamp),
        'America/Sao_Paulo'
    ) as datetime_partida,
    datetime(
        safe_cast(json_value(content, '$.datetime_processamento') as timestamp),
        'America/Sao_Paulo'
    ) as datetime_processamento,
    /* A API envia o fornecedor em caixa mista (ex.: "conecta", "MAXTRACK") */
    lower(safe_cast(json_value(content, '$.fornecedor') as string)) as fornecedor,
    safe_cast(json_value(content, '$.id_veiculo') as string) as id_veiculo,
    safe_cast(json_value(content, '$.route_id') as string) as route_id,
    safe_cast(json_value(content, '$.sentido') as string) as sentido,
    /* A API envia o direction_id como decimal (ex.: 0.0) */
    safe_cast(
        safe_cast(json_value(content, '$.direction_id') as float64) as int64
    ) as direction_id,
    safe_cast(json_value(content, '$.servico') as string) as servico,
    safe_cast(json_value(content, '$.shape_id') as string) as shape_id,
    safe_cast(json_value(content, '$.trip_id') as string) as trip_id,
    safe_cast(json_value(content, '$.tipo_viagem') as string) as tipo_viagem,
    safe_cast(
        json_value(content, '$.tipo_execucao_viagem') as string
    ) as tipo_execucao_viagem
from {{ source("source_conecta", "viagem_informada_sistema_rio") }}
