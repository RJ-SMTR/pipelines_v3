-- Garante a semântica da seleção gulosa de sobreposição (cadeia A–B–C):
-- A elimina B; C não se sobrepõe a A e deve permanecer.
-- Retorna linhas só se o resultado diferir de {A, C}.
with
    candidatas as (
        select *
        from
            unnest(
                [
                    struct(
                        'A' as id_viagem,
                        date('2026-07-01') as data,
                        1 as prioridade,
                        datetime('2026-07-01 08:00:00') as datetime_partida,
                        datetime('2026-07-01 09:00:00') as datetime_chegada
                    ),
                    struct(
                        'B' as id_viagem,
                        date('2026-07-01') as data,
                        2 as prioridade,
                        datetime('2026-07-01 08:50:00') as datetime_partida,
                        datetime('2026-07-01 09:10:00') as datetime_chegada
                    ),
                    struct(
                        'C' as id_viagem,
                        date('2026-07-01') as data,
                        3 as prioridade,
                        datetime('2026-07-01 09:00:00') as datetime_partida,
                        datetime('2026-07-01 10:00:00') as datetime_chegada
                    )
                ]
            )
    ),
    ordenadas as (
        select *, row_number() over (order by data, prioridade) as rn from candidatas
    ),
    -- Desenrola a seleção gulosa para o caso fixo de 3 viagens (sem RECURSIVE,
    -- porque o dbt envolve singular tests em subquery).
    aceita_1 as (select * from ordenadas where rn = 1),
    aceita_2 as (
        select o.*
        from ordenadas as o
        where
            o.rn = 2
            and not exists (
                select 1
                from aceita_1 as a
                where
                    datetime_diff(
                        least(o.datetime_chegada, a.datetime_chegada),
                        greatest(o.datetime_partida, a.datetime_partida),
                        second
                    )
                    > 0
            )
    ),
    aceitas_ate_2 as (
        select *
        from aceita_1
        union all
        select *
        from aceita_2
    ),
    aceita_3 as (
        select o.*
        from ordenadas as o
        where
            o.rn = 3
            and not exists (
                select 1
                from aceitas_ate_2 as a
                where
                    datetime_diff(
                        least(o.datetime_chegada, a.datetime_chegada),
                        greatest(o.datetime_partida, a.datetime_partida),
                        second
                    )
                    > 0
            )
    ),
    obtido as (
        select id_viagem
        from aceitas_ate_2
        union all
        select id_viagem
        from aceita_3
    ),
    esperado as (select id_viagem from unnest(['A', 'C']) as id_viagem),
    divergencias as (
        select 'faltou' as tipo, e.id_viagem
        from esperado as e
        left join obtido as o using (id_viagem)
        where o.id_viagem is null

        union all

        select 'sobrou' as tipo, o.id_viagem
        from obtido as o
        left join esperado as e using (id_viagem)
        where e.id_viagem is null
    )

select *
from divergencias
