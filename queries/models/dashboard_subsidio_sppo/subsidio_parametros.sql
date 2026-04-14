{{
    config(
        materialized="table",
    )
}}

select
    coalesce(safe_cast(indicador_licenciado as bool), false) indicador_licenciado,
    coalesce(
        safe_cast(indicador_ar_condicionado as bool), false
    ) indicador_ar_condicionado,
    coalesce(
        safe_cast(indicador_autuacao_ar_condicionado as bool), false
    ) indicador_autuacao_ar_condicionado,
    coalesce(
        safe_cast(indicador_autuacao_seguranca as bool), false
    ) indicador_autuacao_seguranca,
    coalesce(
        safe_cast(indicador_autuacao_limpeza as bool), false
    ) indicador_autuacao_limpeza,
    coalesce(
        safe_cast(indicador_autuacao_equipamento as bool), false
    ) indicador_autuacao_equipamento,
    coalesce(
        safe_cast(indicador_sensor_temperatura as bool), false
    ) indicador_sensor_temperatura,
    coalesce(safe_cast(indicador_validador_sbd as bool), false) indicador_validador_sbd,
    coalesce(
        safe_cast(indicador_registro_agente_verao_ar_condicionado as bool), false
    ) indicador_registro_agente_verao_ar_condicionado,
    safe_cast(status as string) status,
    safe_cast(subsidio_km as float64) subsidio_km,
    safe_cast(irk as float64) irk,
    safe_cast(data_inicio as date) data_inicio,
    safe_cast(data_fim as date) data_fim,
    safe_cast(legislacao as string) legislacao,
    safe_cast(ordem as int64) ordem
from {{ var("subsidio_parametros") }}
