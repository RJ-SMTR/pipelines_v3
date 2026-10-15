{{ config(alias="ordem_servico_trajeto_alternativo_sentido") }}

select
    safe_cast(data_versao as date) as data_versao,
    safe_cast(servico as string) as servico,
    safe_cast(json_value(content, '$.consorcio') as string) as consorcio,
    safe_cast(tipo_os as string) as tipo_os,
    safe_cast(json_value(content, '$.vista') as string) as vista,
    safe_cast(sentido as string) as sentido,
    safe_cast(json_value(content, '$.extensao') as string) as extensao,
    safe_cast(evento as string) as evento,
    safe_cast(json_value(content, '$.descricao') as string) as descricao,
    safe_cast(json_value(content, '$.ativacao') as string) as ativacao,
    timestamp_captura
from {{ source("source_gtfs", "ordem_servico_trajeto_alternativo_sentido") }}
