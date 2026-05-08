{{
    config(
        alias="planta",
    )
}}

select
    p.data,
    safe_cast(p.planta as string) as id_planta,
    replace(
        safe_cast(json_value(content, '$.tpveic') as string), '.0', ''
    ) as id_tipo_veiculo,
    replace(
        safe_cast(json_value(content, '$.cod_mod_carroceria') as string), '.0', ''
    ) as id_modelo_carroceria,
    replace(
        safe_cast(json_value(content, '$.cod_mod_chassi') as string), '.0', ''
    ) as id_modelo_chassi,
    replace(
        safe_cast(json_value(content, '$.tptran') as string), '.0', ''
    ) as id_tipo_transporte,
    safe_cast(json_value(content, '$.processo') as string) as processo,
    replace(
        safe_cast(json_value(content, '$.lotacao_sentado') as string), '.0', ''
    ) as lotacao_sentado,
    replace(
        safe_cast(json_value(content, '$.lotacao_em_pe') as string), '.0', ''
    ) as lotacao_em_pe,
    date(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S', safe_cast(json_value(content, '$.dtproc') as string)
        )
    ) as data_processo,
    replace(safe_cast(json_value(content, '$.portas') as string), '.0', '') as portas,
    safe_cast(json_value(content, '$.observ') as string) as observacao,
    safe_cast(json_value(content, '$.distee') as float64) as distancia_eixos,
    safe_cast(
        json_value(content, '$.deffis') as boolean
    ) as indicador_deficiente_fisico,
    safe_cast(json_value(content, '$.codplanfab') as string) as id_planta_fabricante,
    safe_cast(
        json_value(content, '$.ar_condicionado') as boolean
    ) as indicador_ar_condicionado,
    safe_cast(json_value(content, '$.piso_baixo') as boolean) as indicador_piso_baixo,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S%Ez',
            safe_cast(json_value(content, '$._datetime_execucao_flow') as string)
        ),
        "America/Sao_Paulo"
    ) as datetime_execucao_flow,
    safe_cast(timestamp_captura as datetime) as datetime_captura
from {{ source("source_stu", "planta") }} as p