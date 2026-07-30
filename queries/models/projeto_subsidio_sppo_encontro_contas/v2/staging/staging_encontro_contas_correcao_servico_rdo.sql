select
    * except (data, receita_tarifaria, tipo, data_resposta, servico, servico_corrigido),
    parse_date("%d/%m/%Y", data) as data,
    parse_date("%d/%m/%Y", data_resposta) as data_resposta,
    safe_cast(
        replace(regexp_replace(receita_tarifaria, r"[^\d,]", ""), ",", ".") as numeric
    ) as receita_tarifaria_aferida_rdo,
    trim(tipo) as tipo,
    case
        when length(trim(servico)) < 3
        then lpad(trim(servico), 3, "0")
        else
            concat(
                ifnull(regexp_extract(trim(servico), r"[A-Z]+"), ""),
                ifnull(regexp_extract(trim(servico), r"[0-9]+"), "")
            )
    end as servico,
    trim(servico_corrigido) as servico_corrigido,
from
    {{
        source(
            "projeto_subsidio_sppo_encontro_contas_staging",
            "rdo_rioonibus_correcao_servico_2024_2025",
        )
    }}
