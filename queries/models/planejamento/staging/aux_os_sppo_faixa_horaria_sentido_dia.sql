{{
    config(
        materialized="ephemeral",
    )
}}

{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

{% set calendario = ref("calendario") %}
{# {% set calendario = "rj-smtr.planejamento.calendario" %} #}
with
    os as (
        select
            *,
            split(faixa_horaria_inicio, ":") as faixa_horaria_inicio_parts,
            split(faixa_horaria_fim, ":") as faixa_horaria_fim_parts
        {# from `rj-smtr.planejamento.ordem_servico_faixa_horaria` #}
        from {{ ref("ordem_servico_faixa_horaria") }}
    ),
    os_tratamento_horario as (
        select
            *,
            make_interval(
                hour => cast(faixa_horaria_inicio_parts[0] as integer),
                minute => cast(faixa_horaria_inicio_parts[1] as integer),
                second => cast(faixa_horaria_inicio_parts[2] as integer)
            ) as faixa_horario_intervalo_inicio,
            make_interval(
                hour => cast(faixa_horaria_fim_parts[0] as integer),
                minute => cast(faixa_horaria_fim_parts[1] as integer),
                second => cast(faixa_horaria_fim_parts[2] as integer)
            ) as faixa_horario_intervalo_fim
        from os
    ),
    os_faixa_horaria_dia as (
        select
            c.data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            extensao_ida,
            extensao_volta,
            viagens_dia,
            c.data + o.faixa_horario_intervalo_inicio as faixa_horaria_inicio,
            c.data + o.faixa_horario_intervalo_fim as faixa_horaria_fim,
            partidas_ida,
            partidas_volta,
            quilometragem,
            partidas
        from {{ calendario }} as c
        join
            os_tratamento_horario as o using (
                feed_version, feed_start_date, tipo_dia, tipo_os
            )
        where {{ incremental_filter }}
    ),
    faixas_agregadas as (
        select
            * except (partidas_ida, partidas_volta, partidas, quilometragem),
            case
                when lag(faixa_horaria_inicio) over (win) = faixa_horaria_inicio
                then lag(partidas_ida) over (win) + partidas_ida
                else partidas_ida
            end as partidas_ida,
            case
                when lag(faixa_horaria_inicio) over (win) = faixa_horaria_inicio
                then lag(partidas_volta) over (win) + partidas_volta
                else partidas_volta
            end as partidas_volta,
            case
                when lag(faixa_horaria_inicio) over (win) = faixa_horaria_inicio
                then lag(partidas) over (win) + partidas
                else partidas
            end as partidas,
            case
                when lag(faixa_horaria_inicio) over (win) = faixa_horaria_inicio
                then lag(quilometragem) over (win) + quilometragem
                else quilometragem
            end as quilometragem
        from os_faixa_horaria_dia
        window win as (partition by servico order by data, faixa_horaria_inicio)
    ),
    os_filtrada as (
        select *
        from faixas_agregadas
        where data = extract(date from faixa_horaria_inicio)
        qualify
            row_number() over (
                partition by servico, faixa_horaria_inicio order by data desc
            )
            = 1
    ),
    os_por_sentido as (
        select
            data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            sentido.codigo as sentido,
            sentido.extensao as extensao,
            sentido.partidas as partidas,
            quilometragem,
            faixa_horaria_inicio,
            faixa_horaria_fim
        from
            os_filtrada,
            unnest(
                [
                    struct(
                        'I' as codigo,
                        extensao_ida as extensao,
                        partidas_ida as partidas
                    ),
                    struct(
                        'V' as codigo,
                        extensao_volta as extensao,
                        partidas_volta as partidas
                    )
                ]
            ) as sentido
    ),
    os_faixa_horaria_sentido as (
        select
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            left(sentido, 1) as sentido,
            extensao,
            partidas,
            quilometragem,
            split(faixa_horaria_inicio, ":") as faixa_horaria_inicio_parts,
            split(faixa_horaria_fim, ":") as faixa_horaria_fim_parts
        {# from `rj-smtr.planejamento.ordem_servico_faixa_horaria_sentido` #}
        from {{ ref("ordem_servico_faixa_horaria_sentido") }}
    ),
    os_faixa_horaria_sentido_tratamento_horario as (
        select
            *,
            make_interval(
                hour => cast(faixa_horaria_inicio_parts[0] as integer),
                minute => cast(faixa_horaria_inicio_parts[1] as integer),
                second => cast(faixa_horaria_inicio_parts[2] as integer)
            ) as faixa_horario_intervalo_inicio,
            make_interval(
                hour => cast(faixa_horaria_fim_parts[0] as integer),
                minute => cast(faixa_horaria_fim_parts[1] as integer),
                second => cast(faixa_horaria_fim_parts[2] as integer)
            ) as faixa_horario_intervalo_fim
        from os_faixa_horaria_sentido
    ),
    os_faixa_horaria_sentido_dia as (
        select
            c.data,
            feed_version,
            feed_start_date,
            tipo_dia,
            tipo_os,
            servico,
            consorcio,
            sentido,
            extensao,
            partidas,
            quilometragem,
            c.data + o.faixa_horario_intervalo_inicio as faixa_horaria_inicio,
            c.data + o.faixa_horario_intervalo_fim as faixa_horaria_fim,
        from {{ calendario }} as c
        join
            os_faixa_horaria_sentido_tratamento_horario as o using (
                feed_version, feed_start_date, tipo_dia, tipo_os
            )
        where {{ incremental_filter }}
    ),
    os_por_sentido_completa as (
        select *
        from os_por_sentido

        union all

        select *
        from os_faixa_horaria_sentido_dia
    )
select *
from os_por_sentido_completa
