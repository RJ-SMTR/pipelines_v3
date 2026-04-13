{{
    config(
        materialized="table",
    )
}}

select
    date(a.data_inicio) as data_inicio,
    date_sub(
        date(lead(a.data_inicio) over (order by a.data_inicio)), interval 1 day
    ) as data_fim,
    cast(a.valor_tarifa as numeric) as valor_tarifa,
    a.legislacao
from
    unnest(
        [
            struct(
                "2023-01-07" as data_inicio,
                4.3 as valor_tarifa,
                "DECRETO RIO Nº 51914 DE 2 DE JANEIRO DE 2023" as legislacao
            ),
            struct(
                "2025-01-05" as data_inicio,
                4.7 as valor_tarifa,
                "DECRETO RIO Nº 55631 DE 1º DE JANEIRO DE 2025" as legislacao
            ),
            struct(
                "2026-01-04" as data_inicio,
                5.0 as valor_tarifa,
                "DECRETO RIO Nº 57473 DE 29 DE DEZEMBRO DE 2025" as legislacao
            )
        ]
    ) as a
