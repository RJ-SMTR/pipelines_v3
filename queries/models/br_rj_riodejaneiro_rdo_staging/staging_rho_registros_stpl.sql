{{
    config(
        alias="rho_registros_stpl",
    )
}}

select
    safe_cast(operadora as string) operadora,
    safe_cast(linha as string) linha,
    safe_cast(data_transacao as date) data_transacao,
    safe_cast(hora_transacao as int64) hora_transacao,
    safe_cast(total_gratuidades as int64) total_gratuidades,
    safe_cast(total_pagantes as int64) total_pagantes,
    safe_cast(codigo as string) codigo,
    safe_cast(
        datetime(timestamp(timestamp_captura), "America/Sao_Paulo") as datetime
    ) timestamp_captura,
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(dia as int64) dia,
    date(concat(ano, '-', mes, '-', dia)) data_particao
from {{ source("br_rj_riodejaneiro_rdo_staging", "rho_registros_stpl") }} as t
