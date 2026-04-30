{{ config(materialized="ephemeral") }}

{%- if execute %}
    {% set query = (
        "SELECT DISTINCT feed_start_date FROM "
        ~ ref("subsidio_data_versao_efetiva")
        ~ " WHERE data BETWEEN DATE('"
        ~ var("start_date")
        ~ "') AND DATE('"
        ~ var("end_date")
        ~ "')"
    ) %}
    {{- log(query, info=True) -}}
    {% set feed_start_dates = run_query(query).columns[0].values() %}
    {{- log(feed_start_dates, info=True) -}}
{% endif -%}

{% set incremental_filter %}
    data between greatest(date("{{var('start_date')}}"), date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}"))  and date("{{ var('end_date') }}")
{% endset %}
with
    -- Viagens planejadas (agrupadas por data e serviço)
    planejado as (
        select distinct
            data,
            split(tipo_dia, " - ")[0] as tipo_dia,
            consorcio,
            servico,
            sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            partidas_total_planejada as viagens_planejadas,
            distancia_total_planejada as km_planejada,
        from {{ ref("viagem_planejada") }}
        -- from `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
        where
            {{ incremental_filter }}
            and (distancia_total_planejada > 0 or distancia_total_planejada is null)
            and (id_tipo_trajeto = 0 or id_tipo_trajeto is null)
    ),
    data_versao_efetiva as (
        select data, tipo_dia, tipo_os, feed_start_date
        from {{ ref("subsidio_data_versao_efetiva") }}
        -- from `rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva`
        -- (alterar também query no bloco execute)
        where {{ incremental_filter }}
    ),
    -- Parâmetros de subsídio
    subsidio_parametros as (
        select distinct
            data_inicio,
            data_fim,
            status,
            tecnologia,
            subsidio_km,
            case
                when tecnologia is null
                then
                    max(subsidio_km) over (
                        partition by date_trunc(data_inicio, year), data_fim
                    )
                when tecnologia is not null
                then
                    max(subsidio_km) over (
                        partition by date_trunc(data_inicio, year), data_fim, tecnologia
                    )
            end as subsidio_km_teto,
            indicador_penalidade_judicial,
            ordem
        from {{ ref("valor_km_tipo_viagem") }}
    ),
    -- Viagens com quantidades de transações
    viagem_transacao as (
        select *
        from {{ ref("viagem_transacao") }}
        -- from `rj-smtr.subsidio.viagem_transacao`
        where {{ incremental_filter }}
    ),
    -- Apuração de km realizado e Percentual de Operação por Faixa Horária (POF)
    servico_faixa_km_apuracao as (
        select
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            consorcio,
            servico,
            sentido,
            km_planejada_faixa as km_planejada,
            pof
        from {{ ref("percentual_operacao_faixa_horaria") }}
        where {{ incremental_filter }}
    ),
    viagem_km_tipo as (
        select distinct
            vt.data,
            vt.servico,
            vt.sentido,
            vt.tipo_viagem,
            vt.tecnologia_apurada,
            vt.tecnologia_remunerada,
            vt.id_viagem,
            vt.datetime_partida,
            vt.distancia_planejada,
            case
                when vt.tipo_viagem = "Não autorizado por capacidade"
                then 0
                else sp.subsidio_km
            end as subsidio_km,
            sp.subsidio_km_teto,
            case
                when
                    vt.data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
                    and s.pof >= 80
                    and vt.tipo_viagem in (
                        "Licenciado com ar e não autuado",
                        "Licenciado sem ar e não autuado"
                    )
                then
                    safe_cast(
                        - (
                            ta.subsidio_km * vt.distancia_planejada
                            - sp.subsidio_km * vt.distancia_planejada
                        ) as numeric
                    )
                else safe_cast(0 as numeric)
            end as valor_glosado_tecnologia,
            if(
                vt.tipo_viagem = "Não autorizado por capacidade", true, false
            ) as indicador_penalidade_tecnologia,
            sp.indicador_penalidade_judicial,
            sp.ordem
        from viagem_transacao as vt
        left join
            subsidio_parametros as sp
            on vt.data between sp.data_inicio and sp.data_fim
            and vt.tipo_viagem = sp.status
            and vt.data >= date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
        left join
            subsidio_parametros as ta
            on vt.data between ta.data_inicio and ta.data_fim
            and vt.tipo_viagem = ta.status
            and (
                vt.data
                between date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}') and date_sub(
                    date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}'), interval 1 day
                )
                and (
                    vt.tecnologia_apurada = ta.tecnologia
                    or (vt.tecnologia_apurada is null and ta.tecnologia is null)
                )
            )
        left join
            servico_faixa_km_apuracao as s
            on s.data = vt.data
            and s.servico = vt.servico
            and s.sentido = vt.sentido
            and vt.datetime_partida
            between s.faixa_horaria_inicio and s.faixa_horaria_fim

    )
-- Flag de viagens que serão consideradas ou não para fins de remuneração (apuração de
-- valor de subsídio) - RESOLUÇÃO SMTR Nº 3645/2023
select
    v.* except (
        rn,
        datetime_partida,
        viagens_planejadas,
        km_planejada,
        tipo_dia,
        consorcio,
        faixa_horaria_inicio,
        faixa_horaria_fim
    ),
    case
        when
            (
                v.data = date('2026-04-02')
                and v.faixa_horaria_inicio >= "2026-04-02T18:00:00"
                and v.faixa_horaria_fim <= "2026-04-02T23:59:59"
            )
        then true  -- Processo 000399006676/2026-28
        when
            (v.data between date('2026-03-16') and date('2026-03-31'))
            and v.servico in (
                "104",
                "107",
                "161",
                "167",
                "169",
                "232",
                "361",
                "409",
                "410",
                "416",
                "435",
                "461",
                "473",
                "552",
                "583",
                "584",
                "805",
                "SP805",
                "LECD127",
                "LECD128",
                "LECD129",
                "LECD130",
                "LECD131",
                "LECD132",
                "LECD133",
                "LECD134",
                "LECD136",
                "LECD137",
                "LECD138"
            )  -- Processo n° 000300.013241/2026-81
        then true
        when
            (v.data = date('2026-03-05'))
            and v.servico in ("321", "324", "325", "326", "329", "616")
        then true  -- Processo: 000399.002687/2026-39
        when
            (v.data between date('2026-02-16') and date('2026-03-15'))
            and v.servico in (
                "104",
                "107",
                "161",
                "167",
                "169",
                "232",
                "361",
                "409",
                "410",
                "416",
                "435",
                "461",
                "473",
                "552",
                "583",
                "584",
                "805",
                "SP805",
                "LECD127",
                "LECD128",
                "LECD129",
                "LECD130",
                "LECD131",
                "LECD132",
                "LECD133",
                "LECD134",
                "LECD136",
                "LECD137",
                "LECD138"
            )  -- Processo n° 000300.005989/2026-19 e Processo SEI: 000300.009756/2026-87
        then true
        when
            (v.data between date('2026-01-16') and date('2026-02-15'))
            and v.servico in (
                "104",
                "107",
                "161",
                "167",
                "169",
                "232",
                "361",
                "409",
                "410",
                "416",
                "435",
                "461",
                "473",
                "552",
                "583",
                "584",
                "SP805",
                "LECD127",
                "LECD128",
                "LECD129",
                "LECD130",
                "LECD131",
                "LECD132",
                "LECD133",
                "LECD134"
            )  -- Processo n° 000300.005989/2026-19
        then true
        when
            (v.data between date('2026-02-01') and date('2026-02-15'))
            and v.servico in ("805", "LECD136", "LECD137")  -- Processo n° 000300.005989/2026-19
        then true
        when
            (v.data between date('2026-01-01') and date('2026-01-15'))
            and v.servico in (
                "104",
                "107",
                "109",
                "161",
                "167",
                "169",
                "409",
                "410",
                "435",
                "473",
                "583",
                "584",
                "LECD127",
                "LECD128",
                "552",
                "SP805",
                "361",
                "LECD129",
                "232"
            )  -- Processo n° 000300.001720/2026-55
        then true
        when
            (v.data between date('2025-12-22') and date('2025-12-26'))
            and v.servico in (
                "232",
                "552",
                "SP805",
                "361",
                "104",
                "107",
                "161",
                "169",
                "409",
                "410",
                "435",
                "473",
                "583",
                "584",
                "109"
            )  -- Processo nº 000300.000641/2026-27
        then true
        when
            (v.data between date('2025-12-29') and date('2025-12-31'))
            and v.servico in (
                "232",
                "552",
                "SP805",
                "361",
                "104",
                "107",
                "161",
                "169",
                "409",
                "410",
                "435",
                "473",
                "583",
                "584",
                "109",
                "167",
                "LECD127",
                "LECD128",
                "LECD129"
            )  -- Processo nº 000300.000641/2026-27
        then true
        when
            v.data = date('2025-09-16')
            and v.servico in ("161", "LECD110", "583", "584", "109")  -- Processo.rio MTR-OFI-2025/06240
        then true
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V17_INICIO") }}')
            and v.tipo_dia in ("Sabado", "Domingo")
            and pof > 120
            and rn > greatest((viagens_planejadas * 1.2), (viagens_planejadas + 1))
        then false
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V17_INICIO") }}')
            and v.tipo_dia = "Ponto Facultativo"
            and pof > 150
            and rn > greatest((viagens_planejadas * 1.5), (viagens_planejadas + 1))
        then false
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V17_INICIO") }}')
            and v.tipo_dia = "Dia Útil"
            and pof > 110
            and rn > greatest((viagens_planejadas * 1.1), (viagens_planejadas + 1))
        then false
        else true
    end as indicador_viagem_dentro_limite,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from
    (
        select
            v.*,
            p.* except (data, servico, sentido),
            row_number() over (
                partition by
                    v.data,
                    v.servico,
                    v.sentido,
                    faixa_horaria_inicio,
                    faixa_horaria_fim
                order by ordem, datetime_partida
            ) as rn
        from viagem_km_tipo as v
        left join
            planejado as p
            on p.data = v.data
            and p.servico = v.servico
            and p.sentido = v.sentido
            and v.datetime_partida
            between p.faixa_horaria_inicio and p.faixa_horaria_fim
    ) as v
left join
    servico_faixa_km_apuracao as s
    on s.data = v.data
    and s.servico = v.servico
    and s.sentido = v.sentido
    and v.datetime_partida between s.faixa_horaria_inicio and s.faixa_horaria_fim
