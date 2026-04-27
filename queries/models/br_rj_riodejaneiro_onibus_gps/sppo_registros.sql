select
    safe_cast(ordem as string) ordem,
    safe_cast(replace(latitude, ',', '.') as float64) latitude,
    safe_cast(replace(longitude, ',', '.') as float64) longitude,
    safe_cast(
        datetime(timestamp(datahora), "America/Sao_Paulo") as datetime
    ) timestamp_gps,
    safe_cast(velocidade as int64) velocidade,
    concat(
        ifnull(regexp_extract(linha, r'[A-Z]+'), ""),
        ifnull(regexp_extract(linha, r'[0-9]+'), "")
    ) as linha,
    safe_cast(
        datetime(timestamp(timestamp_captura), "America/Sao_Paulo") as datetime
    ) timestamp_captura,
    safe_cast(data as date) data,
    safe_cast(hora as int64) hora
from {{ var("sppo_registros_staging") }} as t
