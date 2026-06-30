{{ config(materialized="view") }}

with
    metricas as (
        select
            *,
            row_number() over (
                partition by test_unique_id order by detected_at desc, created_at desc
            ) as ordem_resultado
        from {{ ref("view_metricas_testes_dbt") }}
    )

select * except (ordem_resultado)
from metricas
where ordem_resultado = 1
