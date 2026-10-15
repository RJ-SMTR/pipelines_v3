{{ config(alias="perfil_funcionamento_excecao") }}


with
    excecao as (
        select
            data,
            nullif(safe_cast(area_codigo as string), '') as area_codigo,
            nullif(
                safe_cast(perfil_funcionamento_codigo as string), ''
            ) as perfil_funcionamento_codigo,
            safe.parse_date(
                '%d/%m/%Y',
                safe_cast(
                    json_value(
                        content, '$.perfil_funcionamento_excecao_data_inicio'
                    ) as string
                )
            ) as data_inicio,
            safe.parse_date(
                '%d/%m/%Y',
                safe_cast(
                    json_value(
                        content, '$.perfil_funcionamento_excecao_data_fim'
                    ) as string
                )
            ) as data_fim,
            coalesce(
                safe.parse_time(
                    '%H:%M:%S',
                    safe_cast(
                        json_value(
                            content, '$.perfil_funcionamento_excecao_horario_inicio'
                        ) as string
                    )
                ),
                safe.parse_time(
                    '%H:%M',
                    safe_cast(
                        json_value(
                            content, '$.perfil_funcionamento_excecao_horario_inicio'
                        ) as string
                    )
                )
            ) as horario_inicio,
            coalesce(
                safe.parse_time(
                    '%H:%M:%S',
                    safe_cast(
                        json_value(
                            content, '$.perfil_funcionamento_excecao_horario_fim'
                        ) as string
                    )
                ),
                safe.parse_time(
                    '%H:%M',
                    safe_cast(
                        json_value(
                            content, '$.perfil_funcionamento_excecao_horario_fim'
                        ) as string
                    )
                )
            ) as horario_fim,
            safe_cast(
                json_value(content, '$.perfil_funcionamento_excecao_motivo') as string
            ) as perfil_funcionamento_excecao_motivo,
            nullif(
                safe_cast(perfil_funcionamento_excecao_decisao as string), ''
            ) as perfil_funcionamento_excecao_decisao,
            safe_cast(
                json_value(content, '$.ultimo_editor') as string
            ) as ultimo_editor,
            datetime(
                parse_timestamp(
                    '%Y-%m-%d %H:%M:%S',
                    safe_cast(json_value(content, '$.ultima_atualizacao') as string)
                )
            ) as ultima_atualizacao,
            datetime(
                parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
                "America/Sao_Paulo"
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
    )
select
    data,
    area_codigo,
    perfil_funcionamento_codigo,
    /* sem horário, a exceção vale o dia inteiro nas datas de borda */
    if(
        data_inicio is not null,
        datetime(data_inicio, ifnull(horario_inicio, time "00:00:00")),
        null
    ) as perfil_funcionamento_excecao_datetime_inicio,
    if(
        data_fim is not null,
        datetime(data_fim, ifnull(horario_fim, time "23:59:59")),
        null
    ) as perfil_funcionamento_excecao_datetime_fim,
    perfil_funcionamento_excecao_motivo,
    perfil_funcionamento_excecao_decisao,
    ultimo_editor,
    ultima_atualizacao,
    datetime_captura
from excecao
