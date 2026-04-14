{{ config(materialized="view", alias="licenciamento_stu") }}

select
    data,
    safe_cast(
        datetime(
            timestamp_trunc(timestamp(timestamp_captura), second), "America/Sao_Paulo"
        ) as datetime
    ) timestamp_captura,
    safe_cast(json_value(content, "$.modo") as string) modo,
    safe_cast(id_veiculo as string) id_veiculo,
    safe_cast(json_value(content, "$.ano_fabricacao") as int64) ano_fabricacao,
    safe_cast(json_value(content, "$.carroceria") as string) carroceria,
    case
        when json_value(content, "$.data_ultima_vistoria") = ""
        then null
        else
            safe_cast(
                parse_datetime(
                    "%d/%m/%Y", json_value(content, "$.data_ultima_vistoria")
                ) as date
            )
    end as data_ultima_vistoria,
    safe_cast(json_value(content, "$.id_carroceria") as int64) id_carroceria,
    safe_cast(json_value(content, "$.id_chassi") as int64) id_chassi,
    safe_cast(
        json_value(content, "$.id_fabricante_chassi") as int64
    ) id_fabricante_chassi,
    safe_cast(
        json_value(content, "$.id_interno_carroceria") as int64
    ) id_interno_carroceria,
    safe_cast(json_value(content, "$.id_planta") as int64) id_planta,
    safe_cast(
        json_value(content, "$.indicador_ar_condicionado") as bool
    ) indicador_ar_condicionado,
    safe_cast(json_value(content, "$.indicador_elevador") as bool) indicador_elevador,
    safe_cast(json_value(content, "$.indicador_usb") as bool) indicador_usb,
    safe_cast(json_value(content, "$.indicador_wifi") as bool) indicador_wifi,
    safe_cast(json_value(content, "$.nome_chassi") as string) nome_chassi,
    safe_cast(json_value(content, "$.permissao") as string) permissao,
    safe_cast(json_value(content, "$.placa") as string) placa,
    safe_cast(
        json_value(content, "$.quantidade_lotacao_pe") as int64
    ) quantidade_lotacao_pe,
    safe_cast(
        json_value(content, "$.quantidade_lotacao_sentado") as int64
    ) quantidade_lotacao_sentado,
    safe_cast(json_value(content, "$.tipo_combustivel") as string) tipo_combustivel,
    safe_cast(json_value(content, "$.tipo_veiculo") as string) tipo_veiculo,
    safe_cast(json_value(content, "$.status") as string) status,
    case
        when json_value(content, "$.data_inicio_vinculo") = ""
        then null
        else
            safe_cast(
                parse_datetime(
                    "%d/%m/%Y", json_value(content, "$.data_inicio_vinculo")
                ) as date
            )
    end as data_inicio_vinculo,
    safe_cast(json_value(content, "$.ultima_situacao") as string) ultima_situacao,
    safe_cast(json_value(content, "$.ano_ultima_vistoria") as int64) ano_ultima_vistoria
from {{ source("veiculo_staging", "licenciamento_stu") }} as t
