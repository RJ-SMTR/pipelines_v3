{{
    config(
        alias="gps_validador",
    )
}}


select
    data,
    hora,
    replace(safe_cast(id as string), ".0", "") as id,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    safe_cast(
        json_value(content, '$.bytes_recebidos_app') as float64
    ) as bytes_recebidos_app,
    safe_cast(
        json_value(content, '$.bytes_recebidos_geral') as float64
    ) as bytes_recebidos_geral,
    safe_cast(
        json_value(content, '$.bytes_transmitidos_app') as float64
    ) as bytes_transmitidos_app,
    safe_cast(
        json_value(content, '$.bytes_transmitidos_geral') as float64
    ) as bytes_transmitidos_geral,
    replace(
        safe_cast(json_value(content, '$.codigo_linha_veiculo') as string), ".0", ""
    ) as codigo_linha_veiculo,
    replace(
        safe_cast(json_value(content, '$.codigo_operadora') as string), ".0", ""
    ) as codigo_operadora,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E*S%Ez',
            safe_cast(json_value(content, '$.data_tracking') as string)
        ),
        'America/Sao_Paulo'
    ) as data_tracking,
    safe_cast(
        json_value(content, '$.estado_equipamento') as string
    ) as estado_equipamento,
    safe_cast(
        json_value(content, '$.fabricante_equipamento') as string
    ) as fabricante_equipamento,
    safe_cast(
        json_value(content, '$.latitude_equipamento') as float64
    ) as latitude_equipamento,
    safe_cast(
        json_value(content, '$.longitude_equipamento') as float64
    ) as longitude_equipamento,
    safe_cast(
        json_value(content, '$.modelo_equipamento') as string
    ) as modelo_equipamento,
    safe_cast(
        json_value(content, '$.numero_cartao_operador') as string
    ) as numero_cartao_operador,
    safe_cast(json_value(content, '$.numero_chip_sam') as string) as numero_chip_sam,
    safe_cast(
        json_value(content, '$.numero_chip_telefonia') as string
    ) as numero_chip_telefonia,
    safe_cast(
        json_value(content, '$.numero_serie_equipamento') as string
    ) as numero_serie_equipamento,
    replace(
        safe_cast(json_value(content, '$.prefixo_veiculo') as string), ".0", ""
    ) as prefixo_veiculo,
    safe_cast(
        json_value(content, '$.qtd_transacoes_enviadas') as float64
    ) as qtd_transacoes_enviadas,
    safe_cast(
        json_value(content, '$.qtd_transacoes_pendentes') as float64
    ) as qtd_transacoes_pendentes,
    safe_cast(json_value(content, '$.qtd_venda_botao') as float64) as qtd_venda_botao,
    safe_cast(json_value(content, '$.sentido_linha') as string) as sentido_linha,
    safe_cast(json_value(content, '$.tarifa_linha') as float64) as tarifa_linha,
    safe_cast(json_value(content, '$.versao_app') as string) as versao_app,
    safe_cast(json_value(content, '$.temperatura') as float64) as temperatura
from {{ source("source_jae", "gps_validador") }}
