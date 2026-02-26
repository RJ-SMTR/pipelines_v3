{{ config(materialized="ephemeral") }}

select
    data,
    hora,
    modo,
    consorcio,
    id_servico_jae,
    servico_jae,
    descricao_servico_jae,
    sentido,
    id_transacao,
    cadastro_cliente,
    produto,
    tipo_transacao,
    tipo_usuario,
    meio_pagamento,
    geo_point_transacao,
    valor_pagamento / 0.96 as valor_pagamento
from {{ ref("transacao") }}
where
    id_servico_jae not in ("140", "142")
    and id_operadora != "2"
    and (
        modo = "BRT"
        or (modo = "VLT" and data >= date("2024-02-24"))
        or (modo = "Ônibus" and data >= date("2024-04-19"))
        or (modo = "Van" and consorcio = "STPC" and data >= date("2024-07-01"))
        or (modo = "Van" and consorcio = "STPL" and data >= date("2024-07-15"))
        or (modo = "Van" and consorcio = "TEC" and data >= date("2025-01-01"))
        or (modo = "Metrô" and consorcio = "METRÔ" and data >= date("2025-08-02"))
    )
    and tipo_transacao is not null

union all

select
    t.data,
    t.hora,
    t.modo,
    t.consorcio,
    t.id_servico_jae,
    t.servico_jae,
    t.descricao_servico_jae,
    t.sentido,
    t.id_transacao,
    "Não Cadastrado" as cadastro_cliente,
    "RioCard" as produto,
    "RioCard" as tipo_transacao,
    "RioCard" as tipo_usuario,
    "RioCard" as meio_pagamento,
    st_geogpoint(t.longitude, t.latitude) as geo_point_transacao,
    (
        case
            when t.data < "2025-08-02"
            then null
            when t.sentido = "0"
            then lt.tarifa_ida
            when t.sentido = "1"
            then lt.tarifa_volta
        end
    ) as valor_pagamento,
from {{ ref("transacao_riocard") }} t
left join
    {{ ref("aux_linha_tarifa") }} lt
    on t.id_servico_jae = lt.cd_linha
    and t.data >= lt.dt_inicio_validade
    and (lt.data_fim_validade is null or t.data < lt.data_fim_validade)
where
    (id_servico_jae not in ("140", "142") or id_servico_jae is null)
    and (id_operadora != "2" or id_operadora is null)
    and (
        modo = "BRT"
        or (modo = "VLT" and data >= date("2024-02-24"))
        or (modo = "Ônibus" and data >= date("2024-04-19"))
        or (modo = "Van" and consorcio = "STPC" and data >= date("2024-07-01"))
        or (modo = "Van" and consorcio = "STPL" and data >= date("2024-07-15"))
        or (modo = "Van" and consorcio = "TEC" and data >= date("2025-01-01"))
        or (modo = "Metrô" and consorcio = "METRÔ" and data >= date("2025-08-02"))
        or modo is null
    )
