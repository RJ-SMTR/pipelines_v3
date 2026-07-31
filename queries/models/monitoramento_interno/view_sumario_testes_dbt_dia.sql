{{ config(materialized="view") }}

with metricas as (select * from {{ ref("view_metricas_testes_dbt") }})

select
    data,
    coalesce(flow_name, "sem_flow_name") as flow_name,
    indicador_freshness,
    indicador_hourly,
    tipo_teste,
    subtipo_teste,
    dataset,
    tabela,
    count(*) as quantidade_execucoes,
    count(distinct test_unique_id) as quantidade_testes,
    countif(indicador_sucesso) as quantidade_sucesso,
    countif(indicador_alerta) as quantidade_alerta,
    countif(indicador_erro) as quantidade_erro,
    countif(indicador_nao_sucesso) as quantidade_nao_sucesso,
    safe_divide(countif(indicador_sucesso), count(*)) as taxa_sucesso,
    safe_divide(countif(indicador_nao_sucesso), count(*)) as taxa_nao_sucesso,
    max(datetime_detectado) as datetime_ultima_execucao
from metricas
group by
    data,
    flow_name,
    indicador_freshness,
    indicador_hourly,
    tipo_teste,
    subtipo_teste,
    dataset,
    tabela
