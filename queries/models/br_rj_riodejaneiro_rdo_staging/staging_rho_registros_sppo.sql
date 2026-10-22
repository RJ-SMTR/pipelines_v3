{{
    config(
        alias="rho_registros_sppo",
    )
}}

-- depends_on: {{ ref("staging_rdo_registros_stpl") }}
select
    concat(
        trim(linha_rcti),
        '_',
        data_transacao,
        '_',
        hora_transacao,
        ano,
        '_',
        mes,
        '_',
        dia
    ) id_transacao,
    trim(linha) linha,
    safe_cast(data_transacao as date) data_transacao,
    safe_cast(hora_transacao as int64) hora_transacao,
    safe_cast(total_gratuidades as int64) total_gratuidades,
    safe_cast(total_pagantes_especie as int64) total_pagantes_especie,
    safe_cast(total_pagantes_cartao as int64) total_pagantes_cartao,
    safe_cast(registro_processado as string) registro_processado,
    safe_cast(data_processamento as date) data_processamento,
    safe_cast(operadora as string) operadora,
    safe_cast(linha_rcti as string) linha_rcti,
    datetime(timestamp(timestamp_captura), "America/Sao_Paulo") as timestamp_captura,
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(dia as int64) dia,
    date(concat(ano, '-', mes, '-', dia)) data_particao
from {{ source("br_rj_riodejaneiro_rdo_staging", "rho_registros_sppo") }}
