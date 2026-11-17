{{
    config(
        description="As partidas e a quilometragem das viagens planejadas do SPPO no GTFS correspondem às da Ordem de Serviço para cada feed_start_date, tipo_dia, tipo_os, servico, sentido e faixa horária. Ausências totais de partidas no GTFS sempre acusam, exceto o Ponto Facultativo suspenso a partir do feed 2026-10-19; divergências são toleradas nos feeds/dias com exceção aprovada (eleições, ponto facultativo e todo o feed 2026-11-14, origem: Plano de Verão)."
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
    /* Uma coluna booleana por erro catalogado, exposta como booleano (true/false) no select final */
    diagnostico as (
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
            o.quilometragem as quilometragem_ordem_servico,
            /* Divergência de partidas entre GTFS e OS (qualquer direção) */
            coalesce(v.partidas, 0)
            != coalesce(o.partidas, 0) as indicador_partidas_divergente,
            /* Divergência de quilometragem acima de 0.1 km */
            abs(coalesce(v.quilometragem, 0) - coalesce(o.quilometragem, 0))
            > 0.1 as indicador_quilometragem_divergente,
            /* GTFS sem partidas numa faixa em que a OS tem serviço: a faixa não entra em
               servico_planejado_faixa_horaria e gera as divergências de km reportadas pela CMO.
               Acusa sempre, mesmo nos feeds/dias tolerados (Ponto Facultativo / eleições).
               Faixas em que a própria OS não tem partidas são ignoradas (podem divergir legitimamente). */
            v.partidas is null
            and coalesce(o.partidas, 0) > 0 as indicador_ausencia_partidas_gtfs
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
    quilometragem_ordem_servico,
    indicador_partidas_divergente,
    indicador_quilometragem_divergente,
    indicador_ausencia_partidas_gtfs
from diagnostico
where
    /* Partidas do GTFS nulas acusam independentemente das exceções abaixo, porque a faixa fica
       de fora de servico_planejado_faixa_horaria e gera as divergências de km reportadas pela CMO.
       Única exceção: o Ponto Facultativo suspenso por Matheus Dias Magalhães (09/11/2026, grupo
       Dominio de Planejamento) também não acusa ausência até nova comunicação para desativar.
       Divergência de partidas (qualquer direção) ou de km só acusa fora dos feeds/dias
       tolerados listados no bloco abaixo. */
    indicador_ausencia_partidas_gtfs
    -- Ponto Facultativo suspenso (suspensão aprovada por Matheus Dias Magalhães em
    -- 09/11/2026,
    -- grupo Dominio de Planejamento): não acusa nem divergência nem ausência até nova
    -- comunicação para desativar. Por isso basta feed_start_date >= '2026-10-19' com
    -- tipo_dia = 'Ponto Facultativo': já cobre todos os dias declarados facultativos
    -- (19, 24, 26, 30/10 e 01/11) e os subsequentes.
    and not (feed_start_date >= '2026-10-19' and tipo_dia = 'Ponto Facultativo')
    or (
        (indicador_partidas_divergente or indicador_quilometragem_divergente)
        and not (
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
            )
            /* Excepcionalmente no feed 2026-11-14, para todos os tipos de OS
               (origem: Plano de Verão; tolerância pontual, não cumulativa):
               qualquer divergência de partidas e/ou quilometragem em relação à OS é esperada
               (no feed 2026-11-14 o GTFS tem mais partidas/km que a OS), em qualquer direção.
               Feeds posteriores voltam a ser verificados normalmente. A ausência total de
               partidas do GTFS segue acusada pela 1ª condição do where, independente disto. */
            or (feed_start_date = '2026-11-14')
        )
    )
