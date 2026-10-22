{{
    config(
        materialized="table",
    )
}}

select
    data_inicio,
    data_fim,
    perc_km_inferior,
    perc_km_superior,
    tipo_penalidade,
    valor,
    legislacao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("staging_valor_tipo_penalidade") }}
