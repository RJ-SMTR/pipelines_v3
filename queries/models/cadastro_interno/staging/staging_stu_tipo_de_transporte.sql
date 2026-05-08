{{ config(alias="tipo_de_transporte") }}

select
    data,
    safe_cast(tptran as string) as id_tipo_transporte,
    safe_cast(json_value(content, '$.des_tipo_transporte') as string) as descricao,
    safe_cast(
        json_value(content, '$._datetime_execucao_flow') as datetime
    ) as datetime_execucao_flow,
    safe_cast(timestamp_captura as datetime) as datetime_captura
from {{ source("source_stu", "tipo_de_transporte") }}
