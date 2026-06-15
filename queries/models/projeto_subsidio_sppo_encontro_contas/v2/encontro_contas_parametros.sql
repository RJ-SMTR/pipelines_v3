{{
    config(
        partition_by={"field": "data_inicio"},
    )
}}

with
    parametros_agg as (
        select
            min(data_inicio) as data_inicio,
            max(data_fim) as data_fim,
            safe_cast(9 as numeric) as irk,
            safe_cast(5.94 as numeric) as irk_tarifa_publica,
            safe_cast(3.06 as numeric) as subsidio_km
        from {{ ref("valor_km_tipo_viagem") }}
        where data_inicio >= "{{ var('encontro_contas_datas_v2_inicio') }}"
        group by all
    )
select
    *,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from parametros_agg
