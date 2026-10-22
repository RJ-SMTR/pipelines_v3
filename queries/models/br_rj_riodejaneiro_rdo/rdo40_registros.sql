with
    consorcios as (
        select
            id_consorcio,
            case
                when id_consorcio = "221000050" then "Consórcio BRT" else consorcio
            end as consorcio
        from
            -- rj-smtr.cadastro.consorcios
            {{ ref("consorcios") }}
    )
select
    data,
    ano,
    mes,
    dia,
    id_consorcio,
    consorcio,
    linha as servico,
    r.* except (data, ano, mes, dia, termo),
    (
        qtd_grt_idoso
        + qtd_grt_especial
        + qtd_grt_estud_federal
        + qtd_grt_estud_estadual
        + qtd_grt_estud_municipal
        + qtd_grt_rodoviario
        + qtd_buc_1_perna
        + qtd_buc_2_perna_integracao
        + qtd_buc_supervia_1_perna
        + qtd_buc_supervia_2_perna_integracao
        + qtd_cartoes_perna_unica_e_demais
        + qtd_pagamentos_especie
        + qtd_grt_passe_livre_universitario
    ) as qtd_passageiros_total
from {{ source("br_rj_riodejaneiro_rdo", "rdo40_tratado") }} r
left join consorcios c on r.termo = c.id_consorcio
