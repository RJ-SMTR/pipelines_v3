{{
    config(
        materialized="view",
    )
}}

select
    coalesce(id_operadora_stu, id_operadora_jae) as id_operadora,
    coalesce(operadora_stu, operadora_jae) as operadora,
    coalesce(operadora_completo_stu, operadora_completo_jae) as operadora_completo,
    coalesce(modo_jae, modo_stu) as modo,
    modo_stu,
    modo_jae,
    documento,
    tipo_documento,
    id_operadora_stu,
    id_operadora_jae,
    indicador_operador_ativo_jae
from {{ ref("operadora_historico") }}
qualify
    row_number() over (
        partition by id_operadora_jae order by datetime_inicio_validade desc
    )
    = 1
