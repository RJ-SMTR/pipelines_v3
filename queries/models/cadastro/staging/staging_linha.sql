{{
    config(
        alias="linha",
    )
}}

with
    linha as (
        select
            data,
            datetime(
                parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
                "America/Sao_Paulo"
            ) as timestamp_captura,
            replace(safe_cast(cd_linha as string), '.0', '') as cd_linha,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%S%Ez',
                    safe_cast(json_value(content, '$.DT_INCLUSAO') as string)
                ),
                "America/Sao_Paulo"
            ) as datetime_inclusao,
            safe_cast(
                json_value(content, '$.CD_LINHA_OFICIAL') as string
            ) as cd_linha_oficial,
            safe_cast(
                json_value(content, '$.CD_LOCAL_OPERACAO_LINHA') as string
            ) as cd_local_operacao_linha,
            safe_cast(
                json_value(content, '$.CD_TIPO_CATEGORIA_LINHA') as string
            ) as cd_tipo_categoria_linha,
            safe_cast(
                json_value(content, '$.CD_TIPO_LINHA') as string
            ) as cd_tipo_linha,
            safe_cast(
                json_value(content, '$.CD_TIPO_MATRIZ_CALCULO_SUBSIDIO') as string
            ) as cd_tipo_matriz_calculo_subsidio,
            safe_cast(
                json_value(content, '$.IN_SITUACAO_ATIVIDADE') as string
            ) as in_situacao_atividade,
            safe_cast(json_value(content, '$.KM_LINHA') as float64) as km_linha,
            safe_cast(
                json_value(content, '$.LATITUDE_DESTINO') as string
            ) as latitude_destino,
            safe_cast(
                json_value(content, '$.LATITUDE_ORIGEM') as string
            ) as latitude_origem,
            safe_cast(
                json_value(content, '$.LONGITUDE_DESTINO') as string
            ) as longitude_destino,
            safe_cast(
                json_value(content, '$.LONGITUDE_ORIGEM') as string
            ) as longitude_origem,
            safe_cast(json_value(content, '$.NM_LINHA') as string) as nm_linha,
            safe_cast(json_value(content, '$.NR_LINHA') as string) as nr_linha,
            safe_cast(
                json_value(content, '$.QUANTIDADE_SECAO') as string
            ) as quantidade_secao,
            safe_cast(
                json_value(content, '$.GTFS_ROUTE_ID') as string
            ) as gtfs_route_id,
            safe_cast(json_value(content, '$.GTFS_STOP_ID') as string) as gtfs_stop_id
        from
            (
                select *
                from {{ source("source_jae", "linha") }}
                union all
                select *
                from `rj-smtr-dev.source_jae.linha`
            )
    )
select *
from linha
