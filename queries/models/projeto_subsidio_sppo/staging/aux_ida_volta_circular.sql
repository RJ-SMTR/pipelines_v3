{{
    config(
        materialized="view",
    )
}}


select *
from {{ ref("aux_ida_volta_circular_v1") }}
where data < date("{{ var('DATA_SUBSIDIO_V24_INICIO') }}")
full outer union all by name
select *
from {{ ref("aux_ida_volta_circular_v2") }}
where data >= date("{{ var('DATA_SUBSIDIO_V24_INICIO') }}")
