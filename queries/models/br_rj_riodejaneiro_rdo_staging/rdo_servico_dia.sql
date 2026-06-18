{{
    config(
        alias="rdo_servico_dia",
    )
}}

with
    consorcios as (select id_consorcio, consorcio from {{ ref("consorcios") }}),
    rdo as (
        select
            ano,
            mes,
            dia,
            data,
            termo as id_consorcio,
            cs.consorcio,
            class_servico as classe_servico,
            linha,
            tipo_servico,
            ordem_servico,
            tarifa_codigo,
            tarifa_valor,
            sum(frota_determinada) as frota_determinada,
            sum(frota_licenciada) as frota_licenciada,
            sum(frota_operante) as frota_operante,
            sum(qtd_viagens) as viagens,
            sum(qtd_km_cobertos) as km_cobertos,
            sum(qtd_grt_especial) as quantidade_gratuidade_especial,
            sum(qtd_grt_estud_estadual) as quantidade_gratuidade_estadual,
            sum(qtd_grt_estud_federal) as quantidade_gratuidade_federal,
            sum(qtd_grt_idoso) as quantidade_gratuidade_idoso,
            sum(qtd_grt_estud_municipal) as quantidade_gratuidade_municipal,
            sum(
                qtd_grt_passe_livre_universitario
            ) as quantidade_gratuidade_passe_livre_universitario,
            sum(qtd_grt_rodoviario) as quantidade_gratuidade_rodoviario,
            sum(qtd_buc_1_perna) as quantidade_buc_perna_1,
            sum(qtd_buc_2_perna_integracao) as quantidade_buc_perna_2,
            sum(qtd_buc_supervia_1_perna) as quantidade_buc_supervia_perna_1,
            sum(qtd_buc_supervia_2_perna_integracao) as quantidade_buc_supervia_perna_2,
            sum(qtd_cartoes_perna_unica_e_demais) as quantidade_cartao_perna_unica,
            sum(qtd_pagamentos_especie) as quantidade_especie,
            sum(
                qtd_grt_especial
                + qtd_grt_estud_estadual
                + qtd_grt_estud_federal
                + qtd_grt_idoso
                + qtd_grt_estud_municipal
                + qtd_grt_passe_livre_universitario
                + qtd_grt_rodoviario
            ) as quantidade_gratuidade_total,
            sum(
                qtd_grt_especial
                + qtd_grt_estud_estadual
                + qtd_grt_estud_federal
                + qtd_grt_idoso
                + qtd_grt_estud_municipal
                + qtd_grt_passe_livre_universitario
                + qtd_grt_rodoviario
                + qtd_buc_1_perna
                + qtd_buc_2_perna_integracao
                + qtd_buc_supervia_1_perna
                + qtd_buc_supervia_2_perna_integracao
                + qtd_cartoes_perna_unica_e_demais
                + qtd_pagamentos_especie
            ) as quantidade_passageiros_total,
            sum(receita_buc) as receita_buc,
            sum(receita_buc_supervia) as receita_buc_supervia,
            sum(receita_cartoes_perna_unica_e_demais) as receita_cartao_perna_unica,
            sum(receita_especie) as receita_especie,
            sum(
                receita_buc
                + receita_buc_supervia
                + receita_cartoes_perna_unica_e_demais
                + receita_especie
            ) as receita_tarifaria_total
            {{ source("br_rj_riodejaneiro_rdo", "rdo40_tratado") }} as rdo
        left join consorcios cs on rdo.termo = cs.id_consorcio
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
    ),
    rdo_servico_onibus_tratado as (
        select
            *,
            case
                when linha_tratado like "S%" or linha_tratado like "LECD%"
                then linha_tratado
                else concat(tipo_servico_tratado, ordem_servico_tratado, linha_tratado)
            end as servico
        from
            (
                select distinct
                    data,
                    linha,
                    tipo_servico,
                    ordem_servico,
                    case
                        when regexp_contains(linha, r"^LECD[0-9]{2}$")
                        then linha
                        when
                            length(linha) < 3
                            and regexp_extract(linha, r"[A-Z]+") is null
                        then lpad(linha, 3, "0")
                        else linha
                    end as linha_tratado,
                    case
                        when tipo_servico = "B"
                        then "SP"
                        when tipo_servico in ("D", "E", "R", "V", "N", "P")
                        then concat("S", tipo_servico)
                        else ""
                    end as tipo_servico_tratado,
                    case
                        when
                            count(distinct ordem_servico) over (
                                partition by linha, tipo_servico
                            )
                            > 1
                            and ordem_servico = 1
                        then "A"
                        when
                            count(distinct ordem_servico) over (
                                partition by linha, tipo_servico
                            )
                            > 1
                            and ordem_servico = 2
                        then "B"
                        when
                            count(distinct ordem_servico) over (
                                partition by linha, tipo_servico
                            )
                            > 1
                            and ordem_servico = 3
                        then "C"
                        else ""
                    end as ordem_servico_tratado
                from rdo
                where
                    consorcio
                    in ("Internorte", "Intersul", "Santa Cruz", "Transcarioca")
            )
    )
select
    ano,
    mes,
    dia,
    rdo.data,
    id_consorcio,
    consorcio,
    classe_servico,
    ifnull(rs.servico, linha) as servico,  -- TODO: tratar demais consorcios
    rdo.* except (ano, mes, dia, data, id_consorcio, consorcio, classe_servico)
from rdo
left join rdo_servico_onibus_tratado rs using (data, linha, tipo_servico, ordem_servico)
