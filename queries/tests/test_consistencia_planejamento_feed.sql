{{
    config(
        description="As partidas e a quilometragem das viagens planejadas do SPPO no GTFS correspondem às da Ordem de Serviço para cada feed_start_date, tipo_dia, tipo_os, servico, sentido e faixa horária."
    )
}}

{% set feed_filter %}
    feed_start_date = date('{{ var("data_versao_gtfs") }}')
{% endset %}

with
    viagens as (
        select
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            sentido,
            horario_partida,
            extensao
        from {{ ref("viagem_planejada_planejamento") }}
        where {{ feed_filter }} and sistema = 'SPPO'
    ),
    ordem_servico as (
        select
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            left(sentido, 1) as sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            partidas,
            quilometragem
        from {{ ref("ordem_servico_faixa_horaria_sentido") }}
        where {{ feed_filter }}
    ),
    /* Horários no formato HH:MM:SS permitem comparar como string. Viagens fora de qualquer faixa da OS ficam com faixa nula e aparecem como divergência */
    viagens_faixa as (
        select
            v.feed_start_date,
            v.tipo_dia,
            v.tipo_os,
            v.servico,
            v.sentido,
            o.faixa_horaria_inicio,
            o.faixa_horaria_fim,
            count(*) as partidas,
            sum(v.extensao) as quilometragem
        from viagens as v
        left join
            ordem_servico as o
            on v.feed_start_date = o.feed_start_date
            and v.tipo_dia = o.tipo_dia
            and v.tipo_os = o.tipo_os
            and v.servico = o.servico
            and v.sentido = o.sentido
            and v.horario_partida between o.faixa_horaria_inicio and o.faixa_horaria_fim
        group by all
    ),
    depara as (
        select
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            v.partidas as partidas_gtfs,
            o.partidas as partidas_ordem_servico,
            v.quilometragem as quilometragem_gtfs,
            o.quilometragem as quilometragem_ordem_servico
        from viagens_faixa as v
        full outer join
            ordem_servico as o using (
                feed_start_date,
                tipo_dia,
                tipo_os,
                servico,
                sentido,
                faixa_horaria_inicio,
                faixa_horaria_fim
            )
    ),
    diagnostico as (
        select
            *,
            /* Partidas do GTFS nulas EM FAIXA QUE A OS ESPERAVA SERVIÇO: a faixa não entra
               em servico_planejado_faixa_horaria e gera as divergências de km reportadas pela
               CMO. Deve SEMPRE acusar, mesmo nos feeds/dias tolerados (Ponto Facultativo /
               eleições). Faixas em que a própria OS não tem partidas são ignoradas (podem
               divergir legitimamente). */
            partidas_gtfs is null
            and coalesce(partidas_ordem_servico, 0) > 0 as sem_partidas_gtfs,
            /* Divergência de partidas entre GTFS e OS em qualquer direção */
            coalesce(partidas_gtfs, 0)
            != coalesce(partidas_ordem_servico, 0) as tem_partidas_divergente,
            /* Divergência de quilometragem */
            abs(
                coalesce(quilometragem_gtfs, 0)
                - coalesce(quilometragem_ordem_servico, 0)
            )
            > 0.1 as tem_km_divergente,
            /*
                Feeds/dias onde divergência de partidas E de km é esperada e tolerada:
                -
                - Feeds das eleições: OS sem partidas em Dia Útil, Sábado e Ponto Facultativo
                -
                - Exceção aprovada:
                    o GTFS de Ponto Facultativo destes feeds diverge da OS. Em reunião no dia 05-06-2026
                    foram declarados ponto facultativo nos dias 19, 24, 26, 30 de Outubro e 01 de Novembro;
                    -
                    O Matheus Dias Magalhães (09/11/2026, 17:55, grupo Dominio de Planejamento) solicitou
                    para suspender a verificação de Ponto Facultativo a partir do feed de 07-11-2026 até a
                    nova comunicação para desativar. Por isso basta feed_start_date >= '2026-10-19' com
                    tipo_dia = 'Ponto Facultativo' já considera todos acima como verdade.
            */
            (
                feed_start_date in ('2026-10-04', '2026-10-25')
                or (feed_start_date >= '2026-10-19' and tipo_dia = 'Ponto Facultativo')
            ) as divergencia_tolerada
        from depara
    )
select
    feed_start_date,
    tipo_dia,
    tipo_os,
    servico,
    sentido,
    faixa_horaria_inicio,
    faixa_horaria_fim,
    partidas_gtfs,
    partidas_ordem_servico,
    quilometragem_gtfs,
    quilometragem_ordem_servico
from diagnostico
where
    /*
        Partidas do GTFS nulas SEMPRE acusam (furam as exceções), porque a faixa fica de fora
        de servico_planejado_faixa_horaria e gera as divergências de km reportadas pela CMO.
        Divergência de partidas (qualquer direção) ou de km só acusa fora dos feeds/dias tolerados.
    */
    sem_partidas_gtfs
    or ((tem_partidas_divergente or tem_km_divergente) and not divergencia_tolerada)
