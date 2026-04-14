select
    safe_cast(trip_id as string) trip_id,
    replace(content, "None", "") content,
    safe_cast(data_versao as date) data_versao
from {{ var("trips_staging") }} as t
