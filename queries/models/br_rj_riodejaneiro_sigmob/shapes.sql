select
    safe_cast(shape_id as string) shape_id,
    replace(content, "None", '') content,
    safe_cast(data_versao as date) data_versao
from {{ var("shapes_staging") }} as t
