select
    safe_cast(route_id as string) route_id,
    replace(content, "None", '') content,
    safe_cast(data_versao as date) data_versao
from {{ var("routes_staging") }} as t
