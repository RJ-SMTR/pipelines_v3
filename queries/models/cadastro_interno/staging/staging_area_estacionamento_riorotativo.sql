{{ config(alias="area_estacionamento_riorotativo") }}


select
    data,
    safe_cast(area_codigo as string) as area_codigo,
    safe_cast(json_value(content, '$.area_nome') as string) as area_nome,
    safe_cast(
        json_value(content, '$.area_logradouro_logradouro') as string
    ) as area_logradouro,
    safe_cast(
        json_value(content, '$.area_endereco_referencia') as string
    ) as area_endereco_referencia,
    regexp_replace(
        regexp_replace(
            safe_cast(json_value(content, '$.area_poligono') as string),
            r'\b([A-Za-z]+)\s+(?:ZM|Z|M)\b',
            r'\1'
        ),
        r'(-?[0-9.]+(?:[eE][+-]?[0-9]+)?\s+-?[0-9.]+(?:[eE][+-]?[0-9]+)?)(?:\s+-?[0-9.]+(?:[eE][+-]?[0-9]+)?)+',
        r'\1'
    ) as area_poligono,
    safe_cast(json_value(content, '$.area_observacao') as string) as area_observacao,
    safe_cast(json_value(content, '$.area_vaga_total') as int64) as area_vaga_total,
    safe_cast(json_value(content, '$.area_vaga_moto') as int64) as area_vaga_moto,
    safe_cast(json_value(content, '$.area_vaga_idoso') as int64) as area_vaga_idoso,
    safe_cast(json_value(content, '$.area_vaga_pcd') as int64) as area_vaga_pcd,
    safe_cast(
        json_value(content, '$.area_tempo_permanencia_hora') as int64
    ) as area_tempo_permanencia_hora,
    safe_cast(
        json_value(content, '$.area_perfil_funcionamento') as string
    ) as area_perfil_funcionamento,
    safe.parse_date(
        '%d/%m/%Y', safe_cast(json_value(content, '$.data_inicio_vigencia') as string)
    ) as data_inicio_vigencia,
    safe.parse_date(
        '%d/%m/%Y', safe_cast(json_value(content, '$.data_fim_vigencia') as string)
    ) as data_fim_vigencia,
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
from {{ source("source_riorotativo", "area_estacionamento") }}
qualify row_number() over (partition by area_codigo order by datetime_captura desc) = 1
