{{
    config(
        alias="viagem_informada_maxtrack",
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
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%SZ',
            safe_cast(json_value(content, '$.datetime_chegada') as string)
        ),
        'America/Sao_Paulo'
    ) as datetime_chegada,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%SZ',
            safe_cast(json_value(content, '$.datetime_partida') as string)
        ),
        'America/Sao_Paulo'
    ) as datetime_partida,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%SZ',
            safe_cast(json_value(content, '$.datetime_processamento') as string)
        ),
        'America/Sao_Paulo'
    ) as datetime_processamento,
    safe_cast(json_value(content, '$.fornecedor') as string) as fornecedor,
    safe_cast(json_value(content, '$.id_veiculo') as string) as id_veiculo,
    safe_cast(json_value(content, '$.route_id') as string) as route_id,
    safe_cast(json_value(content, '$.sentido') as string) as sentido,
    case
        json_value(content, '$.sentido') when 'I' then 0 when 'C' then 0 when 'V' then 1
    end as direction_id,
    safe_cast(json_value(content, '$.servico') as string) as servico,
    safe_cast(json_value(content, '$.shape_id') as string) as shape_id,
    safe_cast(json_value(content, '$.trip_id') as string) as trip_id,
    safe_cast(json_value(content, '$.tipo_viagem') as string) as tipo_viagem,
    safe_cast(
        json_value(content, '$.tipo_execucao_viagem') as string
    ) as tipo_execucao_viagem
from {{ source("source_maxtrack", "viagem_informada") }}
