{{ config(alias="perfil_funcionamento_excecao_riorotativo") }}


select
    data,
    /*
    a exceção se aplica a uma área OU a um perfil: a pk não preenchida
    pode chegar como string vazia na external table, normaliza para null
    */
    nullif(safe_cast(area_codigo as string), '') as area_codigo,
    nullif(
        safe_cast(perfil_funcionamento_codigo as string), ''
    ) as perfil_funcionamento_codigo,
    safe.parse_date(
        '%d/%m/%Y',
        safe_cast(
            json_value(content, '$.perfil_funcionamento_excecao_data_inicio') as string
        )
    ) as perfil_funcionamento_excecao_data_inicio,
    safe.parse_date(
        '%d/%m/%Y',
        safe_cast(
            json_value(content, '$.perfil_funcionamento_excecao_data_fim') as string
        )
    ) as perfil_funcionamento_excecao_data_fim,
    safe_cast(
        json_value(content, '$.perfil_funcionamento_excecao_horario_inicio') as string
    ) as perfil_funcionamento_excecao_horario_inicio,
    safe_cast(
        json_value(content, '$.perfil_funcionamento_excecao_horario_fim') as string
    ) as perfil_funcionamento_excecao_horario_fim,
    safe_cast(
        json_value(content, '$.perfil_funcionamento_excecao_motivo') as string
    ) as perfil_funcionamento_excecao_motivo,
    nullif(
        safe_cast(perfil_funcionamento_excecao_decisao as string), ''
    ) as perfil_funcionamento_excecao_decisao,
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
from {{ source("source_riorotativo", "perfil_funcionamento_excecao") }}
qualify
    row_number() over (
        partition by
            data,
            area_codigo,
            perfil_funcionamento_codigo,
            perfil_funcionamento_excecao_decisao
        order by datetime_captura desc
    )
    = 1
