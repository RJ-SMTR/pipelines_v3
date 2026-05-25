{{
    config(
        materialized="table",
    )
}}

with
    prioridade_tecnologia as (
        select "MINI" as tecnologia, 1 as prioridade, "Ônibus SPPO" as modo
        union all
        select "MIDI" as tecnologia, 2 as prioridade, "Ônibus SPPO" as modo
        union all
        select "BASICO" as tecnologia, 3 as prioridade, "Ônibus SPPO" as modo
        union all
        select "PADRON" as tecnologia, 4 as prioridade, "Ônibus SPPO" as modo
    )
select *
from prioridade_tecnologia
