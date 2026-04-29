{{
    config(
        alias="veiculo_fiscalizacao_lacre",
    )
}}

select
    data,
    n_o_de_ordem,
    placa,
    parse_date('%d/%m/%Y', data_do_lacre) as data_do_lacre,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    safe_cast(json_value(content, '$.motivo') as string) as motivo,
    safe_cast(json_value(content, '$.processo') as string) as processo,
    replace(
        safe_cast(json_value(content, '$.permissao') as string), '.0', ''
    ) as permissao,
    safe_cast(json_value(content, '$.consorcio') as string) as consorcio,
    safe_cast(
        json_value(content, '$.permissionario_empresa') as string
    ) as permissionario_empresa,
    safe_cast(json_value(content, '$.no_do_auto') as string) as no_do_auto,
    parse_date(
        '%d/%m/%Y', safe_cast(json_value(content, '$.data_do_deslacre') as string)
    ) as data_do_deslacre,
    safe_cast(
        json_value(content, '$.nome_do_fiscal_deslacre') as string
    ) as nome_do_fiscal_deslacre,
    safe_cast(json_value(content, '$.dias_lacrados') as integer) as dias_lacrados,
    safe_cast(json_value(content, '$.motivo_do_lacre') as string) as motivo_do_lacre,
    safe_cast(json_value(content, '$.ultimo_editor') as string) as ultimo_editor,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S',
            safe_cast(json_value(content, '$.ultima_atualizacao') as string)
        )
    ) as ultima_atualizacao
from {{ source("source_veiculo_fiscalizacao", "veiculo_fiscalizacao_lacre") }}
