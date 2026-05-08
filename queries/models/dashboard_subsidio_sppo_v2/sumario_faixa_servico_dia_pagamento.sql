{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}
with
    sumario_faixa_servico_dia_pagamento as (
        select *
        from {{ ref("sumario_faixa_servico_dia_pagamento_v2") }}
        where data >= date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
        full outer union all by name
        select *
        from {{ ref("sumario_faixa_servico_dia_pagamento_v1") }}
        where data < date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
    )
select *, '{{ invocation_id }}' as id_execucao_dbt
from sumario_faixa_servico_dia_pagamento
