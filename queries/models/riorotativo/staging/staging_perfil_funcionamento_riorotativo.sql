{{ config(alias="perfil_funcionamento") }}


select
    data,
    safe_cast(perfil_funcionamento_codigo as string) as perfil_funcionamento_codigo,
    safe_cast(
        json_value(content, '$.perfil_funcionamento_nome') as string
    ) as perfil_funcionamento_nome,
    array(
        select safe_cast(dia_semana as int64)
        from
            unnest(
                json_value_array(
                    json_value(content, '$.perfil_funcionamento_dia_semana')
                )
            ) as dia_semana
    ) as perfil_funcionamento_dia_semana,
    safe_cast(
        json_value(content, '$.perfil_funcionamento_horario_inicio') as string
    ) as perfil_funcionamento_horario_inicio,
    safe_cast(
        json_value(content, '$.perfil_funcionamento_horario_fim') as string
    ) as perfil_funcionamento_horario_fim,
    safe_cast(json_value(content, '$.ultimo_editor') as string) as ultimo_editor,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S',
            safe_cast(json_value(content, '$.ultima_atualizacao') as string)
        )
    ) as ultima_atualizacao,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as datetime_captura
from {{ source("source_riorotativo", "perfil_funcionamento") }}
qualify
    row_number() over (
        partition by data, perfil_funcionamento_codigo order by datetime_captura desc
    )
    = 1
