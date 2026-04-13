{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}
with
    viagem_planejada as (
        select *
        from {{ ref("viagem_planejada_v2") }}
        where data > date("{{ var('DATA_SUBSIDIO_V6_INICIO') }}")
        full outer union all by name
        select *
        from {{ ref("viagem_planejada_v1") }}
        where data <= date("{{ var('DATA_SUBSIDIO_V6_INICIO') }}")
    )
select *, '{{ invocation_id }}' as id_execucao_dbt
from viagem_planejada
