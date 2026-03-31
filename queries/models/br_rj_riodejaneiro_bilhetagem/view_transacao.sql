{{
    config(
        materialized="view",
        alias="transacao",
    )
}}

select
    data,
    hora,
    datetime_transacao,
    datetime_processamento,
    datetime_captura,
    modo,
    id_consorcio,
    consorcio,
    id_operadora,
    operadora,
    id_servico_jae,
    servico_jae,
    descricao_servico_jae,
    sentido,
    id_veiculo,
    id_validador,
    id_cliente,
    hash_cliente,
    id_transacao,
    meio_pagamento_jae as tipo_pagamento,
    tipo_transacao_jae as tipo_transacao,
    tipo_transacao as tipo_transacao_smtr,
    case
        when tipo_transacao_jae = "Gratuidade" then tipo_usuario
    end as tipo_gratuidade,
    null as id_tipo_integracao,
    null as id_integracao,
    latitude,
    longitude,
    geo_point_transacao,
    null as stop_id,
    null as stop_lat,
    null as stop_lon,
    valor_transacao,
    valor_pagamento,
    data_ordem,
    id_ordem_pagamento_servico_operador_dia,
    id_ordem_pagamento_consorcio_operador_dia,
    id_ordem_pagamento_consorcio_dia,
    id_ordem_pagamento,
    versao,
    datetime_ultima_atualizacao
from {{ ref("transacao") }}
