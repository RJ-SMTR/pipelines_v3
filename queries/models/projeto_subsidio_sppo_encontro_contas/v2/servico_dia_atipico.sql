select
    data,
    servico,
    tipo,
    status,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("aux_servico_dia_atipico") }}
where
    incorporado_datalake_house is not true
    and data not in ({{ var("encontro_contas_datas_excecoes").keys() | join(", ") }})  -- Remove datas de exceção que serão desconsideradas no encontro de contas
