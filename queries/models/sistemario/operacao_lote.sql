{{
    config(
        materialized="table",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  Operação contratual do lote (Anexo I.2 Tab.24–26).
  Fonte: seed sistema_referencia_lote.
  Grão: lote × vigência (data_inicio/data_fim).
  frota_estimada ← frota_operante_hp (plano HP); km_referencia ← qr_mensal / 2.
#}
select
    cast(lote as string) as lote,
    cast(frota_operante_hp as float64) as frota_estimada,
    cast(frota_determinada as float64) as frota_determinada,
    cast(viagens_dia_util as float64) as viagens_dia_util,
    cast(km_comercial_dia_util as float64) as km_comercial_dia_util,
    cast(qr_mensal as float64) as qr_mensal,
    cast(qr_mensal as float64) / 2.0 as km_referencia,
    cast(fonte as string) as fonte,
    cast(data_inicio as date) as data_inicio,
    cast(nullif(data_fim, "") as date) as data_fim,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("sistema_referencia_lote") }}
