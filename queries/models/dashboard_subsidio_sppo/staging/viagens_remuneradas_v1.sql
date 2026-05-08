{{
    config(
        materialized="ephemeral",
    )
}}
{%- if execute %}
    {% set query = (
        "SELECT DISTINCT COALESCE(feed_start_date, data_versao_trips, data_versao_shapes, data_versao_frequencies) FROM "
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
    data between date("{{var('start_date')}}") and date("{{ var('end_date') }}") and data < date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
{% endset %}
with
    -- Viagens planejadas (agrupadas por data e serviço)
    planejado as (
        select distinct
            data,
            split(tipo_dia, " - ")[0] as tipo_dia,
            consorcio,
            servico,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            partidas_total_planejada,
            distancia_total_planejada as km_planejada,
            if(sentido = "C", true, false) as indicador_circular
        from {{ ref("viagem_planejada") }}
        -- from `rj-smtr.projeto_subsidio_sppo.viagem_planejada`
        where
            {{ incremental_filter }}
            and (distancia_total_planejada > 0 or distancia_total_planejada is null)
            and (id_tipo_trajeto = 0 or id_tipo_trajeto is null)
            and data >= date('{{ var("DATA_SUBSIDIO_V3A_INICIO") }}')
            and not (
                data between "2023-12-31" and "2024-01-01"
                and servico in ("583", "584")
                and sentido = "I"
            )  -- Alteração para o reprocessamento do TCM - MTR-CAP-2025/03003 (2023-10-01 a 2024-01-31)
    ),
    viagens_planejadas as (
        select
            feed_start_date,
            servico,
            tipo_dia,
            viagens_planejadas,
            partidas_ida,
            partidas_volta,
            tipo_os
        from {{ ref("aux_ordem_servico_diaria") }}
        where feed_start_date in ('{{ feed_start_dates|join("', '") }}')
    ),
    data_versao_efetiva as (
        select
            data,
            tipo_dia,
            tipo_os,
            coalesce(
                feed_start_date,
                data_versao_trips,
                data_versao_shapes,
                data_versao_frequencies
            ) as feed_start_date
        from {{ ref("subsidio_data_versao_efetiva") }}
        -- from `rj-smtr.projeto_subsidio_sppo.subsidio_data_versao_efetiva`
        -- (alterar também query no bloco execute)
        where
            {{ incremental_filter }}
            and data >= date('{{ var("DATA_SUBSIDIO_V3A_INICIO") }}')
    ),
    viagem_planejada as (
        select
            p.data,
            p.tipo_dia,
            p.consorcio,
            p.servico,
            p.faixa_horaria_inicio,
            p.faixa_horaria_fim,
            v.viagens_planejadas,
            p.km_planejada,
            if(
                p.data >= date('{{ var("DATA_SUBSIDIO_V9_INICIO") }}'),
                p.partidas_total_planejada,
                v.partidas_ida + v.partidas_volta
            ) as viagens_planejadas_ida_volta,
            p.indicador_circular
        from planejado as p
        left join data_versao_efetiva as d using (data, tipo_dia)
        left join
            viagens_planejadas as v
            on d.feed_start_date = v.feed_start_date
            and p.tipo_dia = v.tipo_dia
            and p.servico = v.servico
            and (d.tipo_os = v.tipo_os or (d.tipo_os is null and v.tipo_os = "Regular"))
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
    -- from `rj-smtr.subsidio.valor_km_tipo_viagem`
    ),
    -- Viagens com quantidades de transações
    viagem_transacao as (
        select *
        from {{ ref("viagem_transacao") }}
        -- from `rj-smtr.subsidio.viagem_transacao`
        where
            {{ incremental_filter }}
            and data >= date('{{ var("DATA_SUBSIDIO_V3A_INICIO") }}')
    ),
    {% if var("start_date") < var("DATA_SUBSIDIO_V15_INICIO") %}
        tecnologias as (
            select
                inicio_vigencia,
                fim_vigencia,
                servico,
                codigo_tecnologia,
                maior_tecnologia_permitida,
                menor_tecnologia_permitida
            from {{ ref("tecnologia_servico") }}
        ),
        prioridade_tecnologia as (select * from {{ ref("tecnologia_prioridade") }}),
        viagem_tecnologia as (
            select distinct
                vt.data,
                vt.servico,
                vt.tipo_viagem,
                vt.tecnologia_apurada,
                case
                    when p.prioridade > p_maior.prioridade
                    then t.maior_tecnologia_permitida
                    when
                        p.prioridade < p_menor.prioridade
                        and data >= date('{{ var("DATA_SUBSIDIO_V17_INICIO") }}')
                    then null
                    else vt.tecnologia_apurada
                end as tecnologia_remunerada,
                vt.id_viagem,
                vt.datetime_partida,
                vt.distancia_planejada,
                case
                    when p.prioridade < p_menor.prioridade then true else false
                end as indicador_penalidade_tecnologia
            from viagem_transacao as vt
            left join
                tecnologias as t
                on vt.servico = t.servico
                and (
                    (vt.data between t.inicio_vigencia and t.fim_vigencia)
                    or (vt.data >= t.inicio_vigencia and t.fim_vigencia is null)
                )
            left join prioridade_tecnologia as p on vt.tecnologia_apurada = p.tecnologia
            left join
                prioridade_tecnologia as p_maior
                on t.maior_tecnologia_permitida = p_maior.tecnologia
            left join
                prioridade_tecnologia as p_menor
                on t.menor_tecnologia_permitida = p_menor.tecnologia
        ),
    {% endif %}
    -- Apuração de km realizado e Percentual de Operação por Faixa Horária (POF)
    servico_faixa_km_apuracao as (
        select
            data,
            tipo_dia,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            consorcio,
            servico,
            km_planejada_faixa as km_planejada,
            pof,
        from {{ ref("percentual_operacao_faixa_horaria") }}
        where {{ incremental_filter }}
    ),
    viagem_km_tipo as (
        select distinct
            vt.data,
            vt.servico,
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
        {% if var("start_date") < var("DATA_SUBSIDIO_V15_INICIO") %}
            from viagem_tecnologia as vt
        {% else %} from viagem_transacao as vt
        {% endif %}
        left join
            subsidio_parametros as sp
            on vt.data between sp.data_inicio and sp.data_fim
            and vt.tipo_viagem = sp.status
            and (
                vt.data >= date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
                or (
                    vt.data >= date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
                    and (
                        vt.tecnologia_remunerada = sp.tecnologia
                        or (vt.tecnologia_remunerada is null and sp.tecnologia is null)
                    )
                )
                or (
                    vt.data < date('{{ var("DATA_SUBSIDIO_V14_INICIO") }}')
                    and sp.tecnologia is null
                )
            )
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
            and vt.datetime_partida
            between s.faixa_horaria_inicio and s.faixa_horaria_fim

    )
-- Flag de viagens que serão consideradas ou não para fins de remuneração (apuração de
-- valor de subsídio) - RESOLUÇÃO SMTR Nº 3645/2023
select
    v.* except (
        rn,
        rn_pos_v15,
        datetime_partida,
        viagens_planejadas,
        viagens_planejadas_ida_volta,
        km_planejada,
        tipo_dia,
        consorcio,
        faixa_horaria_inicio,
        faixa_horaria_fim,
        indicador_circular
    ),
    case
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
            and v.tipo_dia in ("Sabado", "Domingo")
            and pof > 120
            and rn_pos_v15 > greatest(
                (viagens_planejadas_ida_volta * 1.2),
                (viagens_planejadas_ida_volta + if(indicador_circular, 1, 2))
            )
        then false
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
            and v.tipo_dia = "Ponto Facultativo"
            and pof > 150
            and rn_pos_v15 > greatest(
                (viagens_planejadas_ida_volta * 1.5),
                (viagens_planejadas_ida_volta + if(indicador_circular, 1, 2))
            )
        then false
        when
            v.data >= date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}')
            and v.tipo_dia = "Dia Útil"
            and pof > 110
            and rn_pos_v15 > greatest(
                (viagens_planejadas_ida_volta * 1.1),
                (viagens_planejadas_ida_volta + if(indicador_circular, 1, 2))
            )
        then false
        when
            v.data between date('{{ var("DATA_SUBSIDIO_V10_INICIO") }}') and date_sub(
                date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}'), interval 1 day
            )
            and v.tipo_dia = "Dia Útil"
            and viagens_planejadas < 10
            and viagens_planejadas > 5
            and pof > 100
            and rn > (viagens_planejadas_ida_volta + if(indicador_circular, 1, 2))
        then false
        when
            v.data between date('{{ var("DATA_SUBSIDIO_V10_INICIO") }}') and date_sub(
                date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}'), interval 1 day
            )
            and v.tipo_dia = "Dia Útil"
            and viagens_planejadas >= 10
            and pof > 110
            and rn > viagens_planejadas_ida_volta * 1.1
        then false
        when
            v.data between date('{{ var("DATA_SUBSIDIO_V10_INICIO") }}') and date_sub(
                date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}'), interval 1 day
            )
            and v.tipo_dia = "Dia Útil"
            and viagens_planejadas <= 5
            and pof > 200
            and rn > viagens_planejadas_ida_volta * 2
        then false
        when
            v.data between date('{{ var("DATA_SUBSIDIO_V10_INICIO") }}') and date_sub(
                date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}'), interval 1 day
            )
            and v.tipo_dia != "Dia Útil"
            and viagens_planejadas < 5
            and pof > 100
            and rn > (viagens_planejadas_ida_volta + if(indicador_circular, 1, 2))
        then false
        when
            v.data between date('{{ var("DATA_SUBSIDIO_V10_INICIO") }}') and date_sub(
                date('{{ var("DATA_SUBSIDIO_V15_INICIO") }}'), interval 1 day
            )
            and v.tipo_dia != "Dia Útil"
            and viagens_planejadas >= 5
            and pof > 120
            and rn > viagens_planejadas_ida_volta * 1.2
        then false
        when
            v.data between date('{{ var("DATA_SUBSIDIO_V3A_INICIO") }}') and date_sub(
                date('{{ var("DATA_SUBSIDIO_V10_INICIO") }}'), interval 1 day
            )
            and v.tipo_dia = "Dia Útil"
            and viagens_planejadas > 10
            and pof > 120
            and rn > viagens_planejadas_ida_volta * 1.2
        then false
        when
            v.data between date('{{ var("DATA_SUBSIDIO_V3A_INICIO") }}') and date_sub(
                date('{{ var("DATA_SUBSIDIO_V10_INICIO") }}'), interval 1 day
            )
            and v.tipo_dia = "Dia Útil"
            and viagens_planejadas <= 10
            and pof > 200
            and rn > viagens_planejadas_ida_volta * 2
        then false
        when
            v.data between date('{{ var("DATA_SUBSIDIO_V3A_INICIO") }}') and date_sub(
                date('{{ var("DATA_SUBSIDIO_V10_INICIO") }}'), interval 1 day
            )
            and (
                v.tipo_dia = "Dia Útil"
                and (viagens_planejadas is null or pof is null or rn is null)
            )
        then null
        else true
    end as indicador_viagem_dentro_limite,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from
    (
        select
            v.*,
            p.* except (data, servico),
            row_number() over (
                partition by v.data, v.servico, faixa_horaria_inicio, faixa_horaria_fim
                order by subsidio_km * distancia_planejada desc, datetime_partida asc
            ) as rn,
            row_number() over (
                partition by v.data, v.servico, faixa_horaria_inicio, faixa_horaria_fim
                order by ordem, datetime_partida
            ) as rn_pos_v15
        from viagem_km_tipo as v
        left join
            viagem_planejada as p
            on p.data = v.data
            and p.servico = v.servico
            and v.datetime_partida
            between p.faixa_horaria_inicio and p.faixa_horaria_fim
    ) as v
left join
    servico_faixa_km_apuracao as s
    on s.data = v.data
    and s.servico = v.servico
    and v.datetime_partida between s.faixa_horaria_inicio and s.faixa_horaria_fim
