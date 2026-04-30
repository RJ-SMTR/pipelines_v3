{{ config(materialized="ephemeral") }}

{% if var("tipo_materializacao") == "monitoramento" %} {% set interval_minutes = 120 %}
{% elif var("tipo_materializacao") == "subsidio" %} {% set interval_minutes = 30 %}
{% endif %}

{% set incremental_filter %}
    data between date("{{ var('start_date') }}") and date_add(date("{{ var('end_date') }}"), interval 1 day)
and data <= date_add(date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}"), interval 1 day)
{% endset %}

with
    -- Transações Jaé
    transacao as (
        select id_veiculo, servico_jae, datetime_transacao
        from {{ ref("transacao") }}
        -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao`
        where
            {{ incremental_filter }}
            and date(datetime_processamento) - date(datetime_transacao)
            <= interval 6 day
            and modo = "Ônibus"
    ),
    -- Transações RioCard
    transacao_riocard as (
        select id_veiculo, servico_jae, datetime_transacao
        from {{ ref("transacao_riocard") }}
        -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao_riocard`
        where
            {{ incremental_filter }}
            and date(datetime_processamento) - date(datetime_transacao)
            <= interval 6 day
            and modo = "Ônibus"
    ),
    -- Status dos veículos
    veiculos as (
        select data, id_veiculo, status, tecnologia
        from {{ ref("aux_veiculo_dia_consolidada") }}
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
            and data < date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
    ),
    -- Viagens realizadas
    viagem_completa as (
        select
            data,
            servico_realizado as servico,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            id_viagem,
            distancia_planejada,
            sentido,
            "Ônibus SPPO" as modo,
            ve.status as tipo_viagem,
            ve.tecnologia as tecnologia_apurada
        from {{ ref("viagem_completa") }}
        -- from `rj-smtr.projeto_subsidio_sppo.viagem_completa`
        left join veiculos as ve using (data, id_veiculo)
        where
            data
            between date_sub(date("{{ var('start_date') }}"), interval 1 day) and date(
                "{{ var('end_date') }}"
            )
            and data < date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")

    ),
    viagem as (
        select
            data,
            id_viagem,
            id_veiculo,
            datetime_partida,
            datetime_chegada,
            modo,
            tecnologia_apurada,
            tecnologia_remunerada,
            tipo_viagem,
            servico,
            sentido,
            distancia_planejada
        from {{ ref("viagem_classificada") }}
        -- from `rj-smtr-dev.victor__subsidio.viagem_classificada`
        where
            data
            between date_sub(date("{{ var('start_date') }}"), interval 1 day) and date(
                "{{ var('end_date') }}"
            )
            and data >= date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
            and data < date("{{ var('DATA_SUBSIDIO_V17_INICIO') }}")
        -- fmt: off
        full outer union all by name
        -- fmt: on
        select *
        from viagem_completa
    ),
    -- Viagem, para fins de contagem de passageiros, com tolerância de 30 minutos,
    -- limitada pela viagem anterior
    viagem_com_tolerancia_previa as (
        select
            v.*,
            lag(v.datetime_chegada) over (
                partition by v.id_veiculo order by v.datetime_partida
            ) as viagem_anterior_chegada,
            case
                when
                    lag(v.datetime_chegada) over (
                        partition by v.id_veiculo order by v.datetime_partida
                    )
                    is null
                then
                    datetime(
                        timestamp_sub(
                            datetime_partida, interval {{ interval_minutes }} minute
                        )
                    )
                else
                    datetime(
                        timestamp_add(
                            greatest(
                                timestamp_sub(
                                    datetime_partida,
                                    interval {{ interval_minutes }} minute
                                ),
                                lag(v.datetime_chegada) over (
                                    partition by v.id_veiculo
                                    order by v.datetime_partida
                                )
                            ),
                            interval 1 second
                        )
                    )
            end as datetime_partida_com_tolerancia
        from viagem as v
    ),
    -- Considera apenas as viagens realizadas no período de apuração
    viagem_com_tolerancia as (
        select *
        from viagem_com_tolerancia_previa
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
    ),
    -- Contagem de transações Jaé
    transacao_contagem as (
        select
            v.data,
            v.id_viagem,
            count(t.datetime_transacao) as quantidade_transacao,
            countif(
                v.servico != t.servico_jae and t.datetime_transacao > v.datetime_partida
            ) as quantidade_transacao_servico_divergente
        from transacao as t
        join
            viagem_com_tolerancia as v
            on t.id_veiculo = substr(v.id_veiculo, 2)
            and t.datetime_transacao
            between v.datetime_partida_com_tolerancia and v.datetime_chegada
        group by 1, 2
    ),
    -- Contagem de transações RioCard
    transacao_riocard_contagem as (
        select
            v.data,
            v.id_viagem,
            count(tr.datetime_transacao) as quantidade_transacao_riocard,
            countif(
                v.servico != tr.servico_jae
                and tr.datetime_transacao > v.datetime_partida
            ) as quantidade_transacao_riocard_servico_divergente
        from transacao_riocard as tr
        join
            viagem_com_tolerancia as v
            on tr.id_veiculo = substr(v.id_veiculo, 2)
            and tr.datetime_transacao
            between v.datetime_partida_com_tolerancia and v.datetime_chegada
        group by 1, 2
    ),
    -- GPS Validador
    gps_validador as (
        select
            data,
            datetime_gps,
            servico_jae,
            id_veiculo,
            id_validador,
            estado_equipamento,
            latitude,
            longitude
        from {{ ref("gps_validador") }}
        -- from `rj-smtr.br_rj_riodejaneiro_bilhetagem.gps_validador`
        where
            {{ incremental_filter }}
            and (
                (
                    data < date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
                    and (latitude != 0 or longitude != 0)
                )
                or data >= date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
            )
            and date(datetime_captura) - date(datetime_gps) <= interval 6 day
            and modo = "Ônibus"
    ),
    -- Ajusta estado do equipamento
    -- Agrupa mesma posição para mesmo validador e veículo, mantendo
    -- preferencialmente
    -- o estado do equipamento "ABERTO" quanto latitude e longitude for diferente de
    -- (0,0)
    estado_equipamento_aux as (
        select *
        from
            (
                (
                    select
                        data,
                        servico_jae,
                        id_validador,
                        id_veiculo,
                        latitude,
                        longitude,
                        if(
                            count(case when estado_equipamento = "ABERTO" then 1 end)
                            >= 1,
                            "ABERTO",
                            "FECHADO"
                        ) as estado_equipamento,
                        min(datetime_gps) as datetime_gps
                    from gps_validador
                    where
                        (
                            data >= date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
                            and latitude != 0
                            and longitude != 0
                        )
                        or data < date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
                    group by 1, 2, 3, 4, 5, 6
                )
                union all
                (
                    select
                        data,
                        servico_jae,
                        id_validador,
                        id_veiculo,
                        latitude,
                        longitude,
                        estado_equipamento,
                        datetime_gps,
                    from gps_validador
                    where
                        data >= date("{{ var('DATA_SUBSIDIO_V12_INICIO') }}")
                        and latitude = 0
                        and longitude = 0
                )
            )
    ),
    -- Relacionamento entre estado do equipamento e viagem
    gps_validador_viagem as (
        select
            v.data,
            e.datetime_gps,
            v.id_viagem,
            e.id_validador,
            e.estado_equipamento,
            e.latitude,
            e.longitude,
            v.servico,
            e.servico_jae,
        from viagem as v
        left join
            estado_equipamento_aux as e
            on e.id_veiculo = substr(v.id_veiculo, 2)
            and e.datetime_gps between v.datetime_partida and v.datetime_chegada
    ),
    -- Calcula a porcentagem de estado do equipamento "ABERTO" por validador e
    -- viagem
    estado_equipamento_perc as (
        select
            data,
            id_viagem,
            id_validador,
            coalesce(t.quantidade_transacao, 0) as quantidade_transacao,
            coalesce(
                tr.quantidade_transacao_riocard, 0
            ) as quantidade_transacao_riocard,
            coalesce(
                t.quantidade_transacao_servico_divergente, 0
            ) as quantidade_transacao_servico_divergente,
            coalesce(
                tr.quantidade_transacao_riocard_servico_divergente, 0
            ) as quantidade_transacao_riocard_servico_divergente,
            countif(servico != servico_jae) > 0 as indicador_gps_servico_divergente,
            safe_cast(
                trunc(countif(estado_equipamento = "ABERTO") / count(*), 5) as numeric
            ) as percentual_estado_equipamento_aberto,
            (
                countif(estado_equipamento = "ABERTO") / count(*) >= 0.8
                or id_validador is null
            ) as indicador_estado_equipamento_aberto
        from gps_validador_viagem
        left join transacao_contagem as t using (data, id_viagem)
        left join transacao_riocard_contagem as tr using (data, id_viagem)
        group by all
    ),
    validador_tipo_viagem as (
        select
            data,
            id_viagem,
            id_validador,
            case
                when data < date('{{ var("DATA_SUBSIDIO_V12_INICIO") }}')
                then quantidade_transacao_riocard = 0
                else (quantidade_transacao_riocard = 0 and quantidade_transacao = 0)
            end as indicador_sem_transacao,
            indicador_estado_equipamento_aberto,
            case
                when
                    data >= date('{{ var("DATA_SUBSIDIO_V8_INICIO") }}')
                    and (
                        (
                            data < date('{{ var("DATA_SUBSIDIO_V12_INICIO") }}')
                            and (
                                quantidade_transacao_riocard = 0
                                or not indicador_estado_equipamento_aberto
                            )
                        )
                        or (
                            data >= date('{{ var("DATA_SUBSIDIO_V12_INICIO") }}')
                            and data < date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
                            and (
                                (
                                    quantidade_transacao_riocard = 0
                                    and quantidade_transacao = 0
                                )
                                or not indicador_estado_equipamento_aberto
                            )
                        )
                    )
                then 'Sem transação'
                else 'Manter tipo viagem'
            end as tipo_viagem
        from estado_equipamento_perc
    ),
    prioridade_tipo_viagem as (
        select 'Sem transação' as tipo_viagem, 1 as prioridade
        union all
        select 'Manter tipo viagem', 4
    ),
    estado_equipamento as (
        select
            data,
            id_viagem,
            max(indicador_sem_transacao) as indicador_sem_transacao,
            max(
                indicador_estado_equipamento_aberto
            ) as indicador_estado_equipamento_aberto
        from validador_tipo_viagem
        group by 1, 2
    ),
    validadores_agrupados as (
        select
            vtv.data,
            vtv.id_viagem,
            vtv.tipo_viagem,
            array_agg(distinct vtv.id_validador ignore nulls) as id_validador,
            ee.indicador_sem_transacao,
            ee.indicador_estado_equipamento_aberto,
            ptv.prioridade
        from validador_tipo_viagem vtv
        join prioridade_tipo_viagem ptv using (tipo_viagem)
        join estado_equipamento ee using (data, id_viagem)
        group by all
        qualify
            row_number() over (
                partition by vtv.data, vtv.id_viagem order by ptv.prioridade
            )
            = 1
    )
select
    v.data,
    v.id_viagem,
    v.id_veiculo,
    v.servico,
    va.id_validador,
    case
        when
            v.tipo_viagem not in (
                "Licenciado com ar e não autuado", "Licenciado sem ar e não autuado"
            )
            or va.tipo_viagem = "Manter tipo viagem"
        then v.tipo_viagem
        when
            v.data < date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
            and va.tipo_viagem = "Sem transação"
            and not va.indicador_sem_transacao
            and va.indicador_estado_equipamento_aberto
        then v.tipo_viagem
        else va.tipo_viagem
    end as tipo_viagem,
    v.modo,
    v.tecnologia_apurada,
    v.tecnologia_remunerada,
    v.sentido,
    v.distancia_planejada,
    any_value(eep.quantidade_transacao) as quantidade_transacao,
    any_value(eep.quantidade_transacao_riocard) as quantidade_transacao_riocard,
    max(
        eep.percentual_estado_equipamento_aberto
    ) as percentual_estado_equipamento_aberto,
    va.indicador_estado_equipamento_aberto,
    v.datetime_partida_com_tolerancia as datetime_partida_bilhetagem,
    v.datetime_partida,
    v.datetime_chegada,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from viagem_com_tolerancia as v
left join estado_equipamento_perc as eep using (data, id_viagem)
left join validadores_agrupados as va using (data, id_viagem)
group by all
