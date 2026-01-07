{{ config(materialized="ephemeral") }}

select
    cd_linha,
    vl_tarifa_ida as tarifa_ida,
    vl_tarifa_volta as tarifa_volta,
    dt_inicio_validade,
    lead(dt_inicio_validade) over (
        partition by cd_linha order by nr_sequencia
    ) as data_fim_validade
from {{ ref("staging_linha_tarifa") }}
