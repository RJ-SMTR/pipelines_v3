{{
    config(
        materialized="ephemeral",
    )
}}

select *
from {{ ref("aux_ida_volta_circular_v1") }}
where
    data < date("{{ var('DATA_SUBSIDIO_V24_INICIO') }}")
    and data not between date("2025-06-01")
    and date("2025-06-30")
full outer union all by name
select *
from {{ ref("aux_ida_volta_circular_v2") }}
where
    data between date("2025-06-01") and date("2025-06-30")
    or data >= date("{{ var('DATA_SUBSIDIO_V24_INICIO') }}")  -- Exceção devido ao recurso SMTR202507001429
