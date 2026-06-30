{{ config(materialized="view") }}

with
    resultados as (
        select *
        from {{ source("elementary", "elementary_test_results") }}
        where test_type = "dbt_test"
    ),

    metricas as (
        select
            date(detected_at, "America/Sao_Paulo") as data,
            datetime(detected_at, "America/Sao_Paulo") as datetime_detectado,
            datetime(created_at, "America/Sao_Paulo") as datetime_criacao,
            detected_at,
            created_at,
            invocation_id,
            test_execution_id,
            test_unique_id,
            model_unique_id,
            database_name as projeto,
            schema_name as dataset,
            table_name as tabela,
            column_name as coluna,
            concat(database_name, ".", schema_name, ".", table_name) as tabela_completa,
            test_name as teste,
            coalesce(test_alias, test_short_name, test_name) as teste_curto,
            test_type as tipo_teste,
            test_sub_type as subtipo_teste,
            severity as severidade,
            lower(status) as status,
            safe_cast(failures as int64) as quantidade_falhas,
            safe_cast(failed_row_count as int64) as quantidade_linhas_falhas,
            json_value_array(tags) as tags,
            owners as responsaveis,
            test_params as parametros_teste,
            test_results_description as descricao_resultado,
            test_results_query as query_resultado,
            result_rows as linhas_resultado,
            cast(null as string) as flow_name,
            lower(status) = "pass" as indicador_sucesso,
            lower(status) = "warn" as indicador_alerta,
            lower(status) in ("fail", "error") as indicador_erro,
            lower(status) not in ("pass", "skipped") as indicador_nao_sucesso,
            exists (
                select 1
                from unnest(ifnull(json_value_array(tags), [])) as tag
                where tag = "freshness"
            ) as indicador_freshness,
            exists (
                select 1
                from unnest(ifnull(json_value_array(tags), [])) as tag
                where tag = "hourly"
            ) as indicador_hourly
        from resultados
    )

select *
from metricas
