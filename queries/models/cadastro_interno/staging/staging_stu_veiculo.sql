{{ config(alias="veiculo") }}

select
    data,
    placa,
    replace(
        safe_cast(json_value(content, '$.cod_modelo') as string), '.0', ''
    ) as id_modelo,
    replace(
        safe_cast(json_value(content, '$.planta') as string), '.0', ''
    ) as id_planta,
    replace(
        safe_cast(json_value(content, '$.tpveic') as string), '.0', ''
    ) as id_tipo_veiculo,
    replace(safe_cast(json_value(content, '$.cod_cor') as string), '.0', '') as id_cor,
    replace(
        safe_cast(json_value(content, '$.cod_combustivel') as string), '.0', ''
    ) as id_combustivel,
    replace(safe_cast(json_value(content, '$.renavam') as string), '.0', '') as renavam,
    replace(safe_cast(json_value(content, '$.portas') as string), '.0', '') as portas,
    safe_cast(json_value(content, '$.chassi') as string) as chassi,
    safe_cast(
        json_value(content, '$.processo_taximetro') as string
    ) as processo_taximetro,
    replace(
        safe_cast(json_value(content, '$.cod_marca_taximetro') as string), '.0', ''
    ) as id_marca_taximetro,
    safe_cast(json_value(content, '$.num_taximetro') as string) as numero_taximetro,
    safe_cast(json_value(content, '$.num_impressora') as string) as numero_impressora,
    safe_cast(json_value(content, '$.isencao') as boolean) as indicador_isencao,
    safe_cast(json_value(content, '$.situac') as string) as situacao,
    replace(
        safe_cast(json_value(content, '$.ano_fabricacao') as string), '.0', ''
    ) as ano_fabricacao,
    replace(
        safe_cast(json_value(content, '$.ano_fabricacao_carroceria') as string),
        '.0',
        ''
    ) as ano_fabricacao_carroceria,
    replace(
        safe_cast(json_value(content, '$.ano_licenca') as string), '.0', ''
    ) as ano_licenca,
    replace(
        safe_cast(json_value(content, '$.ano_modelo') as string), '.0', ''
    ) as ano_modelo,
    safe_cast(json_value(content, '$.cadastro') as boolean) as indicador_cadastro,
    safe_cast(
        json_value(content, '$.erro_migracao') as boolean
    ) as indicador_erro_migracao,
    safe_cast(json_value(content, '$.NHOMO') as string) as numero_homologacao,
    date(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S',
            safe_cast(json_value(content, '$.dtValidadeGNV') as string)
        )
    ) as data_validade_gnv,
    date(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S',
            safe_cast(json_value(content, '$.DtAquisicao') as string)
        )
    ) as data_aquisicao,
    safe_cast(json_value(content, '$.GPS') as string) as gps,
    safe_cast(json_value(content, '$.Camera') as string) as camera,
    safe_cast(json_value(content, '$.PlacaAntiga') as string) as placa_antiga,
    safe_cast(json_value(content, '$.WiFi') as boolean) as indicador_wifi,
    safe_cast(json_value(content, '$.USB') as boolean) as indicador_usb,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S%Ez',
            safe_cast(json_value(content, '$._datetime_execucao_flow') as string)
        ),
        "America/Sao_Paulo"
    ) as datetime_execucao_flow,
    timestamp_captura
from {{ source("source_stu", "veiculo") }}
