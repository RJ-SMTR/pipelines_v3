{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "id_viagem"],
        incremental_strategy="insert_overwrite",
    )
}}
with
    viagens_remuneradas as (
        select *
        from {{ ref("viagens_remuneradas_v2") }}
        where data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
    {# full outer union all by name
        select *
        from {{ ref("viagens_remuneradas_v1") }}
        where data < date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}") #}
    )
select *, '{{ invocation_id }}' as id_execucao_dbt
from viagens_remuneradas
