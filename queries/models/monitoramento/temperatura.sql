{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    data between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

with
    inmet as (
        select
            data,
            extract(hour from hora) as hora,
            max(temperatura) as temperatura,
            "inmet" as fonte
        from {{ ref("temperatura_inmet") }}
        where
            {{ incremental_filter }} and id_estacao in ("A621", "A652", "A636", "A602")  -- Estações do Rio de Janeiro
        group by 1, 2
    ),
    alertario as (
        select
            data,
            extract(hour from hora) as hora,
            max(temperatura) as temperatura,
            "alertario" as fonte
        from {{ ref("temperatura_alertario") }}
        where {{ incremental_filter }}
        group by 1, 2
    )
select
    coalesce(i.data, a.data) as data,
    coalesce(i.hora, a.hora) as hora,
    coalesce(i.temperatura, a.temperatura) as temperatura,
    case when i.temperatura is not null then i.fonte else a.fonte end as fonte,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from inmet i
full outer join alertario a using (data, hora)
