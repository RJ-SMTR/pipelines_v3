{{
    config(
        materialized="ephemeral",
    )
}}


select *
from {{ ref("aux_ida_volta_circular_v2") }}

