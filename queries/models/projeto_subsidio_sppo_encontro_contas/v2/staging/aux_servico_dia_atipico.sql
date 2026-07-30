select
    parse_date("%d/%m/%Y", data) as data,
    case
        when length(trim(servico)) < 3
        then lpad(trim(servico), 3, "0")
        else
            concat(
                ifnull(upper(regexp_extract(trim(servico), r"[A-z]+")), ""),
                ifnull(regexp_extract(trim(servico), r"[0-9]+"), "")
            )
    end as servico,
    safe_cast(incorporado_bigquery as bool) as incorporado_datalake_house,
    * except (data, servico, incorporado_bigquery, analisado)
from
    {{
        source(
            "projeto_subsidio_sppo_encontro_contas_staging",
            "subtt_cmo_recurso_encontro_contas",
        )
    }}
