{{
    config(
        alias="rdo5_registros_sppo",
    )
}}
select
    safe_cast(operadora as string) operadora,
    safe_cast(linha as string) linha,
    safe_cast(parse_datetime("%Y-%m-%d", data_transacao) as datetime) data_transacao,
    safe_cast(tarifa_valor as float64) tarifa_valor,
    safe_cast(gratuidade_idoso as int64) gratuidade_idoso,
    safe_cast(gratuidade_especial as int64) gratuidade_especial,
    safe_cast(gratuidade_estudante_federal as int64) gratuidade_estudante_federal,
    safe_cast(gratuidade_estudante_estadual as int64) gratuidade_estudante_estadual,
    safe_cast(gratuidade_estudante_municipal as int64) gratuidade_estudante_municipal,
    safe_cast(universitario as int64) universitario,
    safe_cast(gratuito_rodoviario as int64) gratuito_rodoviario,
    safe_cast(buc_1a_perna as int64) buc_1a_perna,
    safe_cast(buc_2a_perna as int64) buc_2a_perna,
    safe_cast(buc_receita as float64) buc_receita,
    safe_cast(buc_supervia_1a_perna as int64) buc_supervia_1a_perna,
    safe_cast(buc_supervia_2a_perna as int64) buc_supervia_2a_perna,
    safe_cast(buc_supervia_receita as float64) buc_supervia_receita,
    safe_cast(buc_van_1a_perna as int64) buc_van_1a_perna,
    safe_cast(buc_van_2a_perna as int64) buc_van_2a_perna,
    safe_cast(buc_van_receita as float64) buc_van_receita,
    safe_cast(buc_vlt_1a_perna as int64) buc_vlt_1a_perna,
    safe_cast(buc_vlt_2a_perna as int64) buc_vlt_2a_perna,
    safe_cast(buc_vlt_receita as float64) buc_vlt_receita,
    safe_cast(buc_brt_1a_perna as int64) buc_brt_1a_perna,
    safe_cast(buc_brt_2a_perna as int64) buc_brt_2a_perna,
    safe_cast(buc_brt_3a_perna as int64) buc_brt_3a_perna,
    safe_cast(buc_brt_receita as float64) buc_brt_receita,
    safe_cast(buc_inter_1a_perna as int64) buc_inter_1a_perna,
    safe_cast(buc_inter_2a_perna as int64) buc_inter_2a_perna,
    safe_cast(buc_inter_receita as float64) buc_inter_receita,
    safe_cast(buc_barcas_1a_perna as int64) buc_barcas_1a_perna,
    safe_cast(buc_barcas_2a_perna as int64) buc_barcas_2a_perna,
    safe_cast(buc_barcas_receita as float64) buc_barcas_receita,
    safe_cast(buc_metro_1a_perna as int64) buc_metro_1a_perna,
    safe_cast(buc_metro_2a_perna as int64) buc_metro_2a_perna,
    safe_cast(buc_metro_receita as float64) buc_metro_receita,
    safe_cast(cartao as int64) cartao,
    safe_cast(receita_cartao as float64) receita_cartao,
    safe_cast(especie_passageiro_transportado as int64) especie_passageiro_transportado,
    safe_cast(especie_receita as float64) especie_receita,
    safe_cast(registro_processado as string) registro_processado,
    safe_cast(
        parse_datetime(
            "%Y%m%d", safe_cast(safe_cast(data_processamento as int64) as string)
        ) as datetime
    ) data_processamento,
    safe_cast(linha_rcti as string) linha_rcti,
    safe_cast(
        datetime(timestamp(timestamp_captura), "America/Sao_Paulo") as datetime
    ) timestamp_captura,
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(dia as int64) dia
from {{ source("br_rj_riodejaneiro_rdo_staging", "rdo5_registros") }} as t
