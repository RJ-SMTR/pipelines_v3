{{
    config(
        alias="laudo_pcd",
    )
}}

select
    data,
    replace(safe_cast(id as string), '.0', '') as id,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    replace(
        safe_cast(json_value(content, '$.cd_cliente') as string), '.0', ''
    ) as cd_cliente,
    safe_cast(json_value(content, '$.codigo_cid') as string) as codigo_cid,
    case
        when
            safe_cast(json_value(content, '$.deficiencia_permanente') as float64)
            is null
        then cast(json_value(content, '$.deficiencia_permanente') as bool)
        else
            cast(
                cast(
                    cast(
                        json_value(content, '$.deficiencia_permanente') as float64
                    ) as integer
                ) as bool
            )
    end as deficiencia_permanente,
    case
        when
            safe_cast(json_value(content, '$.necessita_acompanhante') as float64)
            is null
        then cast(json_value(content, '$.necessita_acompanhante') as bool)
        else
            cast(
                cast(
                    cast(
                        json_value(content, '$.necessita_acompanhante') as float64
                    ) as integer
                ) as bool
            )
    end as necessita_acompanhante,
    case
        when
            safe_cast(json_value(content, '$.desativar_biometria_facial') as float64)
            is null
        then cast(json_value(content, '$.desativar_biometria_facial') as bool)
        else
            cast(
                cast(
                    cast(
                        json_value(content, '$.desativar_biometria_facial') as float64
                    ) as integer
                ) as bool
            )
    end as desativar_biometria_facial,
    replace(
        safe_cast(json_value(content, '$.id_tipo_tempo_tratamento') as string), '.0', ''
    ) as id_tipo_tempo_tratamento,
    safe_cast(
        json_value(content, '$.qt_tempo_tratamento') as integer
    ) as qt_tempo_tratamento,
    replace(
        safe_cast(json_value(content, '$.id_tipo_frequencia_tratamento') as string),
        '.0',
        ''
    ) as id_tipo_frequencia_tratamento,
    safe_cast(
        json_value(content, '$.qt_frequencia_tratamento') as integer
    ) as qt_frequencia_tratamento,
    safe_cast(json_value(content, '$.login_atendente') as string) as login_atendente,
    safe_cast(json_value(content, '$.tx_observacao') as string) as tx_observacao,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.data_inclusao') as string)
        ),
        'America/Sao_Paulo'
    ) as data_inclusao,
    replace(
        safe_cast(json_value(content, '$.id_tipo_doenca') as string), '.0', ''
    ) as id_tipo_doenca,
    safe_cast(
        json_value(content, '$.codigo_uap_tratamento') as string
    ) as codigo_uap_tratamento
from {{ source("source_jae", "laudo_pcd") }}
