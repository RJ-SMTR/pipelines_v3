{{ config(alias=this.name ~ "_" ~ var("fonte_gps")) }}

with
    source_data as (
        select
            data,
            safe_cast(hora as int64) hora,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%SZ',
                    safe_cast(json_value(content, '$.datetime') as string)
                ),
                "America/Sao_Paulo"
            ) datetime_gps,
            safe_cast(id_veiculo as string) id_veiculo,
            concat(
                ifnull(
                    regexp_extract(
                        safe_cast(json_value(content, '$.servico') as string), r'[A-Z]+'
                    ),
                    ""
                ),
                ifnull(
                    regexp_extract(
                        safe_cast(json_value(content, '$.servico') as string), r'[0-9]+'
                    ),
                    ""
                )
            ) as servico,
            safe_cast(json_value(content, '$.sentido') as string) sentido,
            safe_cast(
                replace(json_value(content, '$.latitude'), ',', '.') as float64
            ) latitude,
            safe_cast(
                replace(json_value(content, '$.longitude'), ',', '.') as float64
            ) longitude,
            safe_cast(json_value(content, '$.velocidade') as int64) velocidade,
            safe_cast(json_value(content, '$.direcao') as numeric) direcao,
            safe_cast(json_value(content, '$.route_id') as string) route_id,
            safe_cast(json_value(content, '$.trip_id') as string) trip_id,
            safe_cast(json_value(content, '$.shape_id') as string) shape_id,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%SZ',
                    safe_cast(json_value(content, '$.datetime_envio') as string)
                ),
                "America/Sao_Paulo"
            ) datetime_envio,
            datetime(
                parse_timestamp('%Y-%m-%dT%H:%M:%SZ', datetime_servidor),
                "America/Sao_Paulo"
            ) datetime_servidor,
            datetime(
                parse_timestamp(
                    '%Y-%m-%d %H:%M:%S%Ez',
                    safe_cast(
                        json_value(content, '$._datetime_execucao_flow') as string
                    )
                ),
                "America/Sao_Paulo"
            ) datetime_execucao_flow,
            datetime(
                parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
                "America/Sao_Paulo"
            ) datetime_captura
        from {{ source("source_" ~ var("fonte_gps"), "registros") }}
    ),
    filtered_data as (
        select *
        from source_data
        where
            datetime_diff(datetime_envio, datetime_gps, second) >= -20
            and datetime_diff(datetime_envio, datetime_gps, minute) <= 60
    )
select
    data,
    hora,
    id_veiculo,
    servico,
    sentido,
    latitude,
    longitude,
    datetime_gps,
    velocidade,
    direcao,
    route_id,
    trip_id,
    shape_id,
    datetime_envio,
    datetime_servidor,
    datetime_execucao_flow,
    datetime_captura
from filtered_data
