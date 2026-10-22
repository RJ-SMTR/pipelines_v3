{{ config(materialized="ephemeral") }}

select *
from {{ ref("licenciamento") }}
where tipo_veiculo not like "%ROD%" and modo = 'ONIBUS'
