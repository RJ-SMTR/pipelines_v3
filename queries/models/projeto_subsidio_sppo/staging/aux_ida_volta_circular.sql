{{
    config(
        materialized="ephemeral",
    )
}}


select *
from {{ ref("aux_ida_volta_circular_v1") }}
where
    not (
        data >= date("{{ var('DATA_SUBSIDIO_V24_INICIO') }}")
        or data between date("2025-04-01") and date("2025-04-30")
    )

union all by name

select *
from {{ ref("aux_ida_volta_circular_v2") }}
where
    data >= date("{{ var('DATA_SUBSIDIO_V24_INICIO') }}")
    or data between date("2025-04-01") and date("2025-04-30")
