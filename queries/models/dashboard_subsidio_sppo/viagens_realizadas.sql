select *
from {{ ref("viagem_completa") }}
where data between "2022-06-01" and date("{{ var('end_date') }}")
