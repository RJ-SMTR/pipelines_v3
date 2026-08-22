{{
    config(
        materialized="view",
        alias=this.name ~ "_" ~ var("fonte_gps"),
        tags=["geolocalizacao"],
    )
}}

with
    source_data as (
        select
            date(
                datetime(
                    safe_cast(json_value(content, "$.datetime") as timestamp),
                    "America/Sao_Paulo"
                )
            ) as data,
            extract(
                hour
                from
                    datetime(
                        safe_cast(json_value(content, "$.datetime") as timestamp),
                        "America/Sao_Paulo"
                    )
            ) as hora,
            safe_cast(id_registro as string) as id_registro,
            datetime(
                safe_cast(json_value(content, "$.datetime") as timestamp),
                "America/Sao_Paulo"
            ) as datetime_gps,
            datetime(
                safe_cast(json_value(content, "$.datetime_envio") as timestamp),
                "America/Sao_Paulo"
            ) as datetime_envio,
            datetime(
                safe_cast(json_value(content, "$.datetime_servidor") as timestamp),
                "America/Sao_Paulo"
            ) as datetime_servidor,
            safe_cast(json_value(content, "$.id_veiculo") as string) as id_veiculo,
            safe_cast(
                json_value(content, "$.id_equipamento") as string
            ) as id_equipamento,
            safe_cast(
                json_value(content, "$.sequencial_equipamento") as int64
            ) as sequencial_equipamento,
            safe_cast(json_value(content, "$.route_id") as string) as route_id,
            safe_cast(json_value(content, "$.servico") as string) as servico,
            safe_cast(
                json_value(content, "$.id_viagem_planejada") as string
            ) as id_viagem_planejada,
            safe_cast(json_value(content, "$.trip_id") as string) as trip_id,
            safe_cast(json_value(content, "$.shape_id") as string) as shape_id,
            safe_cast(json_value(content, "$.direction_id") as int64) as direction_id,
            safe_cast(json_value(content, "$.sentido") as string) as sentido,
            safe_cast(
                json_value(content, "$.qualidade_sinal") as string
            ) as qualidade_sinal,
            safe_cast(
                json_value(content, "$.fonte_posicao") as string
            ) as fonte_posicao,
            safe_cast(
                json_value(content, "$.fonte_velocidade") as string
            ) as fonte_velocidade,
            safe_cast(json_value(content, "$.latitude") as float64) as latitude,
            safe_cast(json_value(content, "$.longitude") as float64) as longitude,
            safe_cast(json_value(content, "$.altitude") as float64) as altitude,
            safe_cast(json_value(content, "$.velocidade") as float64) as velocidade,
            safe_cast(json_value(content, "$.direcao") as float64) as direcao,
            safe_cast(
                json_value(content, "$.quantidade_satelites") as int64
            ) as quantidade_satelites,
            safe_cast(json_value(content, "$.hdop") as float64) as hdop,
            safe_cast(json_value(content, "$.vdop") as float64) as vdop,
            safe_cast(json_value(content, "$.pdop") as float64) as pdop,
            datetime(
                safe_cast(timestamp_captura as timestamp), "America/Sao_Paulo"
            ) as datetime_captura
        from {{ source("source_" ~ var("fonte_gps"), "registros") }}
    )
select *
from source_data
where
    datetime_servidor is not null
    and datetime_envio is not null
    and datetime_diff(datetime_servidor, datetime_envio, second) between -20 and 3600
