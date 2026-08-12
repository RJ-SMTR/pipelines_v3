{{
    config(
        materialized="table",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  Operação do lote por tecnologia (Anexo I.2 Tab.24–26).
  Fonte: seed sistema_referencia_lote_tecnologia.
  Grão: lote × vigência × tipo_veiculo.
  Denominador do FCF tipológico em `fcf_quinzena_lote` (I.8 §4).
#}
select
    cast(lote as string) as lote,
    cast(tipo_veiculo as string) as tipo_veiculo,
    cast(frota_operante_hp as float64) as frota_estimada,
    cast(frota_determinada as float64) as frota_determinada,
    cast(viagens_dia_util as float64) as viagens_dia_util,
    cast(km_comercial_dia_util as float64) as km_comercial_dia_util,
    cast(pmd_comercial_dia_util as float64) as pmd_comercial_dia_util,
    cast(pmm_comercial as float64) as pmm_comercial,
    cast(fonte as string) as fonte,
    cast(data_inicio as date) as data_inicio,
    cast(nullif(data_fim, "") as date) as data_fim,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("sistema_referencia_lote_tecnologia") }}
