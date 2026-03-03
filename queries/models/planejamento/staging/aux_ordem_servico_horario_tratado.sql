{{ config(materialized="ephemeral") }}

with
    ordem_servico as (
        select
            * except (horario_inicio, horario_fim),
            split(horario_inicio, ":") horario_inicio_parts,
            split(horario_fim, ":") horario_fim_parts
        from {{ ref("aux_ordem_servico_diaria") }}
    )
select
    * except (horario_fim_parts, horario_inicio_parts),
    case
        when array_length(horario_inicio_parts) = 3
        then
            make_interval(
                hour => cast(horario_inicio_parts[0] as integer),
                minute => cast(horario_inicio_parts[1] as integer),
                second => cast(horario_inicio_parts[2] as integer)
            )
    end as horario_inicio,
    case
        when array_length(horario_fim_parts) = 3
        then
            make_interval(
                hour => cast(horario_fim_parts[0] as integer),
                minute => cast(horario_fim_parts[1] as integer),
                second => cast(horario_fim_parts[2] as integer)
            )
    end as horario_fim
from ordem_servico
