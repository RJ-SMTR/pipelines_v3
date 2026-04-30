{{
    config(
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key=["servico", "feed_start_date"],
        alias="ordem_servico",
    )
}}

with
    ordem_servico as (
        select
            fi.feed_version,
            safe_cast(os.data_versao as date) as feed_start_date,
            fi.feed_end_date,
            safe_cast(tipo_os as string) tipo_os,
            safe_cast(os.servico as string) servico,
            safe_cast(json_value(os.content, '$.vista') as string) vista,
            safe_cast(json_value(os.content, '$.consorcio') as string) consorcio,
            safe_cast(
                json_value(os.content, '$.horario_inicio') as string
            ) horario_inicio,
            safe_cast(json_value(os.content, '$.horario_fim') as string) horario_fim,
            safe_cast(json_value(os.content, '$.extensao_ida') as float64) extensao_ida,
            safe_cast(
                json_value(os.content, '$.extensao_volta') as float64
            ) extensao_volta,
            safe_cast(
                safe_cast(
                    json_value(os.content, '$.partidas_ida_du') as float64
                ) as int64
            ) partidas_ida_du,
            safe_cast(
                safe_cast(
                    json_value(os.content, '$.partidas_volta_du') as float64
                ) as int64
            ) partidas_volta_du,
            safe_cast(json_value(os.content, '$.viagens_du') as float64) viagens_du,
            safe_cast(json_value(os.content, '$.km_dia_util') as float64) km_du,
            safe_cast(
                safe_cast(
                    json_value(os.content, '$.partidas_ida_pf') as float64
                ) as int64
            ) partidas_ida_pf,
            safe_cast(
                safe_cast(
                    json_value(os.content, '$.partidas_volta_pf') as float64
                ) as int64
            ) partidas_volta_pf,
            safe_cast(json_value(os.content, '$.viagens_pf') as float64) viagens_pf,
            safe_cast(json_value(os.content, '$.km_pf') as float64) km_pf,
            safe_cast(
                safe_cast(
                    json_value(os.content, '$.partidas_ida_sabado') as float64
                ) as int64
            ) partidas_ida_sabado,
            safe_cast(
                safe_cast(
                    json_value(os.content, '$.partidas_volta_sabado') as float64
                ) as int64
            ) partidas_volta_sabado,
            safe_cast(
                json_value(os.content, '$.viagens_sabado') as float64
            ) viagens_sabado,
            safe_cast(json_value(os.content, '$.km_sabado') as float64) km_sabado,
            safe_cast(
                safe_cast(
                    json_value(os.content, '$.partidas_ida_domingo') as float64
                ) as int64
            ) partidas_ida_domingo,
            safe_cast(
                safe_cast(
                    json_value(os.content, '$.partidas_volta_domingo') as float64
                ) as int64
            ) partidas_volta_domingo,
            safe_cast(
                json_value(os.content, '$.viagens_domingo') as float64
            ) viagens_domingo,
            safe_cast(json_value(os.content, '$.km_domingo') as float64) km_domingo,
        from {{ source("br_rj_riodejaneiro_gtfs_staging", "ordem_servico") }} os
        join
            {{ ref("feed_info_gtfs") }} fi
            on os.data_versao = cast(fi.feed_start_date as string)
        {% if is_incremental() -%}
            where
                os.data_versao = '{{ var("data_versao_gtfs") }}'
                and fi.feed_start_date = '{{ var("data_versao_gtfs") }}'
        {%- endif %}
    )
select *, '{{ var("version") }}' as versao_modelo
from
    ordem_servico unpivot (
        (
            partidas_ida,
            partidas_volta,
            viagens_planejadas,
            distancia_total_planejada
        ) for tipo_dia in (
            (partidas_ida_du, partidas_volta_du, viagens_du, km_du) as 'Dia Útil',
            (
                partidas_ida_pf, partidas_volta_pf, viagens_pf, km_pf
            ) as 'Ponto Facultativo',
            (
                partidas_ida_sabado, partidas_volta_sabado, viagens_sabado, km_sabado
            ) as 'Sabado',
            (
                partidas_ida_domingo,
                partidas_volta_domingo,
                viagens_domingo,
                km_domingo
            ) as 'Domingo'
        )
    )

where feed_start_date < "{{var('DATA_GTFS_V2_INICIO') }}"
