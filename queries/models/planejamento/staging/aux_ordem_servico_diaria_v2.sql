{{ config(materialized="ephemeral") }}

select
    feed_version,
    feed_start_date,
    feed_end_date,
    tipo_os,
    servico,
    vista,
    consorcio,
    min(faixa_horaria_inicio) as horario_inicio,
    max(faixa_horaria_fim) as horario_fim,
    left(sentido, 1) as sentido,
    extensao,
    sum(partidas) as viagens_planejadas,
    sum(quilometragem) as distancia_total_planejada,
    tipo_dia
from {{ ref("ordem_servico_faixa_horaria_sentido") }}
group by 1, 2, 3, 4, 5, 6, 7, 10, 11, 14
