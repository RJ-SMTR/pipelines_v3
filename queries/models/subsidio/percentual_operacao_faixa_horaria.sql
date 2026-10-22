{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}
with
    percentual_operacao_faixa_horaria as (
        select *
        from {{ ref("percentual_operacao_faixa_horaria_v2") }}
        where data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
        full outer union all by name
        select *
        from {{ ref("percentual_operacao_faixa_horaria_v1") }}
        where data < date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
    )
select *, '{{ invocation_id }}' as id_execucao_dbt
from percentual_operacao_faixa_horaria
