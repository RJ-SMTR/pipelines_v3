{{
    config(
        alias="rdo_registros_stpl",
    )
}}

select
    safe_cast(operadora as string) as operadora,
    safe_cast(linha as string) as linha,
    safe_cast(tarifa_valor as numeric) as tarifa_valor,
    safe_cast(data_transacao as date) as data_transacao,
    safe_cast(gratuidade_idoso as int64) as gratuidade_idoso,
    safe_cast(gratuidade_especial as int64) as gratuidade_especial,
    safe_cast(gratuidade_estudante_federal as int64) as gratuidade_estudante_federal,
    safe_cast(gratuidade_estudante_estadual as int64) as gratuidade_estudante_estadual,
    safe_cast(
        gratuidade_estudante_municipal as int64
    ) as gratuidade_estudante_municipal,
    safe_cast(universitario as int64) as universitario,
    safe_cast(buc_1a_perna as int64) as buc_1a_perna,
    safe_cast(buc_2a_perna as int64) as buc_2a_perna,
    safe_cast(buc_receita as numeric) as buc_receita,
    safe_cast(buc_supervia_1a_perna as int64) as buc_supervia_1a_perna,
    safe_cast(buc_supervia_2a_perna as int64) as buc_supervia_2a_perna,
    safe_cast(buc_supervia_receita as numeric) as buc_supervia_receita,
    safe_cast(buc_van_1a_perna as int64) as buc_van_1a_perna,
    safe_cast(buc_van_2a_perna as int64) as buc_van_2a_perna,
    safe_cast(buc_van_receita as numeric) as buc_van_receita,
    safe_cast(buc_brt_1a_perna as int64) as buc_brt_1a_perna,
    safe_cast(buc_brt_2a_perna as int64) as buc_brt_2a_perna,
    safe_cast(buc_brt_3a_perna as int64) as buc_brt_3a_perna,
    safe_cast(buc_brt_receita as numeric) as buc_brt_receita,
    safe_cast(buc_inter_1a_perna as int64) as buc_inter_1a_perna,
    safe_cast(buc_inter_2a_perna as int64) as buc_inter_2a_perna,
    safe_cast(buc_inter_receita as numeric) as buc_inter_receita,
    safe_cast(buc_metro_1a_perna as int64) as buc_metro_1a_perna,
    safe_cast(buc_metro_2a_perna as int64) as buc_metro_2a_perna,
    safe_cast(buc_metro_receita as numeric) as buc_metro_receita,
    safe_cast(cartao as int64) as cartao,
    safe_cast(receita_cartao as int64) as receita_cartao,
    safe_cast(
        especie_passageiro_transportado as int64
    ) as especie_passageiro_transportado,
    safe_cast(especie_receita as numeric) as especie_receita,
    safe_cast(registro_processado as string) as registro_processado,
    parse_date('%Y%m%d', replace(data_processamento, ".0", "")) as data_processamento,
    safe_cast(linha_rcti as string) linha_rcti,
    safe_cast(codigo as string) codigo,
    safe_cast(
        datetime(timestamp(timestamp_captura), "America/Sao_Paulo") as datetime
    ) timestamp_captura,
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(dia as int64) dia,
    date(concat(ano, '-', mes, '-', dia)) data_particao
from {{ source("br_rj_riodejaneiro_rdo_staging", "rdo_registros_stpl") }} as t
