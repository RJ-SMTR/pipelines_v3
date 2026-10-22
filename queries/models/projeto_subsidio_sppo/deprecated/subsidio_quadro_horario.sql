select
    safe_cast(data_versao as date) data_versao,
    safe_cast(servico as string) servico,
    safe_cast(vista as string) vista,
    safe_cast(consorcio as string) consorcio,
    safe_cast(horario_inicio as string) horario_inicio,
    safe_cast(horario_fim as string) horario_fim,
    safe_cast(trip_id as string) trip_id,
    safe_cast(sentido as string) sentido,
    safe_cast(distancia_planejada as float64) distancia_planejada,
    safe_cast(tipo_dia as string) tipo_dia,
    safe_cast(distancia_total_planejada as float64) distancia_total_planejada
from {{ var("quadro_horario") }} as t
