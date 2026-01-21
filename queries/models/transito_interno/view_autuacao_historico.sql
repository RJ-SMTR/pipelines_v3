{{ config(materialized="view") }}

with
    historico as (
        select
            * except (
                timestamp_ultima_atualizacao,
                dbt_scd_id,
                dbt_updated_at,
                dbt_valid_from,
                dbt_valid_to
            ),
            dbt_scd_id as id_alteracao,
            datetime(dbt_updated_at, "America/Sao_Paulo") as datetime_ultima_alteracao,
            datetime(dbt_valid_from, "America/Sao_Paulo") as datetime_inicio_vigencia,
            datetime(dbt_valid_to, "America/Sao_Paulo") as datetime_fim_vigencia
        from {{ ref("snapshot_transito_autuacao") }}
    )
select *
from historico
