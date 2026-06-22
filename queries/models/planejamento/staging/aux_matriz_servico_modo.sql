{{
    config(
        materialized="ephemeral",
    )
}}

select id_servico_jae, split(modos, ",") as modos
from {{ source("source_smtr_dev", "matriz_servico_modo") }}
