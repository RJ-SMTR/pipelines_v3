with
    ordem_servico as (
        select
            safe_cast(data_versao as date) as data_versao,
            timestamp_captura,
            servico,
            safe_cast(json_value(content, "$.vista") as string) as vista,
            safe_cast(json_value(content, "$.consorcio") as string) as consorcio,
            safe_cast(
                json_value(content, "$.horario_inicio") as string
            ) as horario_inicio,
            safe_cast(json_value(content, "$.horario_fim") as string) as horario_fim,
            safe_cast(json_value(content, "$.extensao_ida") as float64) as extensao_ida,
            safe_cast(
                json_value(content, "$.extensao_volta") as float64
            ) as extensao_volta,
            safe_cast(
                json_value(content, "$.partidas_ida_du") as int64
            ) as partidas_ida_du,
            safe_cast(
                json_value(content, "$.partidas_volta_du") as int64
            ) as partidas_volta_du,
            safe_cast(json_value(content, "$.viagens_du") as float64) as viagens_du,
            safe_cast(json_value(content, "$.km_dia_util") as float64) as km_du,
            safe_cast(
                json_value(content, "$.partidas_ida_pf") as int64
            ) as partidas_ida_pf,
            safe_cast(
                json_value(content, "$.partidas_volta_pf") as int64
            ) as partidas_volta_pf,
            safe_cast(json_value(content, "$.viagens_pf") as float64) as viagens_pf,
            safe_cast(json_value(content, "$.km_pf") as float64) as km_pf,
            null as partidas_ida_sabado,
            null as partidas_volta_sabado,
            safe_cast(null as float64) as viagens_sabado,
            safe_cast(json_value(content, "$.km_sabado") as float64) as km_sabado,
            null as partidas_ida_domingo,
            null as partidas_volta_domingo,
            safe_cast(null as float64) as viagens_domingo,
            safe_cast(json_value(content, "$.km_domingo") as float64) as km_domingo
        from {{ source("projeto_subsidio_sppo_staging", "ordem_servico") }}
    )
select *
from
    ordem_servico unpivot (
        (partidas_ida, partidas_volta, viagens_planejadas, distancia_total_planejada)
        for tipo_dia in (
            (partidas_ida_du, partidas_volta_du, viagens_du, km_du) as "Dia Útil",
            (
                partidas_ida_pf, partidas_volta_pf, viagens_pf, km_pf
            ) as "Ponto Facultativo",
            (
                partidas_ida_sabado, partidas_volta_sabado, viagens_sabado, km_sabado
            ) as "Sabado",
            (
                partidas_ida_domingo,
                partidas_volta_domingo,
                viagens_domingo,
                km_domingo
            ) as "Domingo"
        )
    )
