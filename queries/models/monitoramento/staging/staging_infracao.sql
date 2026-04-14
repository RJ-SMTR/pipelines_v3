{{ config(materialized="view", alias="infracao") }}


with
    infracoes_base as (
        select
            data,
            safe_cast(json_value(content, '$.id_infracao') as string) as id_infracao,
            safe_cast(json_value(content, '$.modo') as string) as modo,
            safe_cast(json_value(content, '$.servico') as string) as servico,
            safe_cast(json_value(content, '$.permissao') as string) as permissao,
            safe_cast(json_value(content, '$.placa') as string) as placa,
            safe_cast(id_auto_infracao as string) as id_auto_infracao,
            safe_cast(
                json_value(content, '$.data_infracao') as string
            ) as raw_data_infracao,
            safe_cast(json_value(content, '$.infracao') as string) as infracao,
            safe_cast(json_value(content, '$.valor') as float64) as valor,
            safe_cast(json_value(content, '$.status') as string) as status,
            safe_cast(
                datetime(
                    timestamp_trunc(timestamp(timestamp_captura), second),
                    "America/Sao_Paulo"
                ) as datetime
            ) as timestamp_captura,
            json_value(content, '$.data_pagamento') as raw_data_pagamento
        from {{ source("veiculo_staging", "infracao") }} as t
    ),

    infracoes_com_datas as (
        select
            *,

            -- Parse apenas a data (sem hora)
            parse_date(
                "%d/%m/%Y", split(raw_data_infracao, " ")[offset(0)]
            ) as data_infracao,

            -- Normaliza e converte para DATETIME (com hora, mesmo que seja 00:00:00)
            parse_datetime(
                "%d/%m/%Y %H:%M:%S",
                case
                    when
                        regexp_contains(
                            raw_data_infracao, r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
                        )
                    then raw_data_infracao
                    when
                        regexp_contains(
                            raw_data_infracao, r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}'
                        )
                    then concat(raw_data_infracao, ':00')
                    else null
                end
            ) as datetime_infracao,

            -- Data de pagamento
            if(
                raw_data_pagamento = "",
                null,
                parse_date("%d/%m/%Y", raw_data_pagamento)
            ) as data_pagamento

        from infracoes_base
    )

select
    data,
    id_infracao,
    modo,
    servico,
    permissao,
    placa,
    id_auto_infracao,
    data_infracao,
    datetime_infracao,
    infracao,
    valor,
    status,
    data_pagamento,
    timestamp_captura
from infracoes_com_datas
