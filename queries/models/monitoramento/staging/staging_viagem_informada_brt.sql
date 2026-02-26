{{
    config(
        alias="viagem_informada_brt",
    )
}}

select
    data,
    safe_cast(id_viagem as string) as id_viagem,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S%Ez',
            safe_cast(json_value(content, '$.timestamp_captura') as string)
        ),
        'America/Sao_Paulo'
    ) as datetime_captura,
    date(
        parse_timestamp(
            '%Y-%m-%d', safe_cast(json_value(content, '$.data_viagem') as string)
        )
    ) as data_viagem,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S',
            safe_cast(json_value(content, '$.datetime_chegada') as string)
        ),
        'America/Sao_Paulo'
    ) as datetime_chegada,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S',
            safe_cast(json_value(content, '$.datetime_partida') as string)
        ),
        'America/Sao_Paulo'
    ) as datetime_partida,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S',
            safe_cast(json_value(content, '$.datetime_processamento') as string)
        ),
        'America/Sao_Paulo'
    ) as datetime_processamento,
    safe_cast(json_value(content, '$.id_veiculo') as string) as id_veiculo,
    safe_cast(json_value(content, '$.route_id') as string) as route_id,
    safe_cast(json_value(content, '$.sentido') as string) as sentido,
    safe_cast(json_value(content, '$.servico') as string) as servico,
    safe_cast(json_value(content, '$.shape_id') as string) as shape_id,
    safe_cast(json_value(content, '$.trip_id') as string) as trip_id
from {{ source("source_sonda", "viagem_informada") }}
