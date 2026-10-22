{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    subsidio_penalidade_servico_faixa as (
        select *
        from {{ ref("subsidio_penalidade_servico_faixa_v2") }}
        where
            data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
         --fmt:off
         full outer union all by name
        --fmt:on
        select *
        from {{ ref("subsidio_penalidade_servico_faixa_v1") }}
        where data < date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
    )
select *, '{{ invocation_id }}' as id_execucao_dbt
from subsidio_penalidade_servico_faixa
