{{
    config(
        materialized="table",
        tags=["remuneracao", "openfisca", "deprecated"],
    )
}}

{#
  DEPRECATED — preferir `viagens_apuradas`.
  Mantém o grão data×lote a partir da frota diária já calculada no OF.
#}
select
    data,
    lote,
    max(indicador_dia_util) as indicador_dia_util,
    max(frota_pico_manha) as frota_pico_manha,
    max(frota_pico_tarde) as frota_pico_tarde,
    max(frota_operante) as frota_operante
from {{ ref("viagens_apuradas") }}
group by data, lote
