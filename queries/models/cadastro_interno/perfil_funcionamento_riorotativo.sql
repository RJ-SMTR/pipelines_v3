{{ config(materialized="table") }}


select
    perfil_funcionamento_codigo as id_perfil_funcionamento,
    perfil_funcionamento_nome as nome,
    perfil_funcionamento_dia_semana as dias_semana,
    perfil_funcionamento_horario_inicio as horario_inicio,
    perfil_funcionamento_horario_fim as horario_fim
from {{ ref("staging_perfil_funcionamento_riorotativo") }}
