{{
    config(
        alias="garagem",
    )
}}


select
    parse_date(
        '%d/%m/%Y', nullif(trim(safe_cast(inicio_vigencia as string)), "")
    ) as inicio_vigencia,
    parse_date(
        '%d/%m/%Y', nullif(trim(safe_cast(fim_vigencia as string)), "")
    ) as fim_vigencia,
    trim(operador) as operador,
    trim(endereco) as endereco,
    trim(bairro) as bairro,
    case
        when upper(trim(oficial)) = "SIM"
        then true
        when upper(trim(oficial)) = "NÃO"
        then false
        else null
    end as oficial,
    case
        when upper(trim(ativa_ou_inativa)) = "ATIVA"
        then true
        when upper(trim(ativa_ou_inativa)) = "INATIVA"
        then false
        else null
    end as indicador_ativa,
    lower(trim(uso)) as tipo_uso,
    trim(obs) as observacao,
    area_m2 as area,
    geometry_wkt
from {{ source("cadastro_staging", "garagem") }}
