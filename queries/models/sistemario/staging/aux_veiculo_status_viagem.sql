{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

with
    viagens as (
        select
            data,
            servico,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            id_viagem,
            distancia_planejada,
            sentido,
            modo
        from {{ ref("viagem_valida") }}
        where {{ incremental_filter }}
    ),
    veiculos as (
        select data, id_veiculo, placa, ano_fabricacao, tecnologia, status, indicadores
        from {{ ref("aux_veiculo_dia_consolidada") }}
        where {{ incremental_filter }}
    ),
    autuacao_disciplinar as (
        select data, datetime_autuacao, id_infracao, servico, placa
        from {{ ref("autuacao_disciplinar_historico") }}
        where
            (
                data_inclusao_datalake <= date_add(data, interval 7 day)
                or data_inclusao_datalake
                = date("{{ var('data_inclusao_autuacao_disciplinar') }}")
            )
            and {{ incremental_filter }}
            and modo = "ONIBUS"
            and status != "Cancelada"
    ),
    tecnologias as (
        select
            inicio_vigencia,
            fim_vigencia,
            servico,
            maior_tecnologia_permitida,
            menor_tecnologia_permitida
        from {{ ref("tecnologia_servico") }}
    ),
    prioridade_tecnologia as (select * from {{ ref("tecnologia_prioridade") }}),
    veiculo_autuacao as (
        select
            ad.data,
            ad.datetime_autuacao,
            ad.id_infracao,
            ad.servico,
            ad.placa,
            ve.id_veiculo
        from autuacao_disciplinar ad
        left join veiculos ve on ad.data = ve.data and ad.placa = ve.placa
    ),
    viagem_status as (
        select
            v.data,
            v.servico,
            v.datetime_partida,
            v.datetime_chegada,
            v.id_veiculo,
            ve.placa,
            ve.ano_fabricacao,
            v.id_viagem,
            v.distancia_planejada,
            v.sentido,
            v.modo,
            ve.tecnologia,
            ve.status,
            ve.indicadores
        from viagens v
        left join veiculos ve on v.data = ve.data and v.id_veiculo = ve.id_veiculo
    ),
    viagem_tecnologia as (
        select
            vs.data,
            vs.servico,
            vs.datetime_partida,
            vs.datetime_chegada,
            vs.id_veiculo,
            vs.placa,
            vs.ano_fabricacao,
            vs.id_viagem,
            vs.distancia_planejada,
            vs.sentido,
            vs.modo,
            vs.status,
            vs.indicadores,
            t.inicio_vigencia as data_inicio_vigencia,
            vs.tecnologia as tecnologia_apurada,
            case
                when p.prioridade > p_maior.prioridade
                then t.maior_tecnologia_permitida
                when
                    p.prioridade < p_menor.prioridade
                    and vs.data >= date('{{ var("DATA_SUBSIDIO_V16_INICIO") }}')
                then null
                else vs.tecnologia
            end as tecnologia_remunerada,
            case
                when p.prioridade < p_menor.prioridade then true else false
            end as indicador_penalidade_tecnologia
        from viagem_status vs
        left join
            tecnologias t
            on vs.servico = t.servico
            and (
                (vs.data between t.inicio_vigencia and t.fim_vigencia)
                or (vs.data >= t.inicio_vigencia and t.fim_vigencia is null)
            )
        left join prioridade_tecnologia as p on vs.tecnologia = p.tecnologia
        left join
            prioridade_tecnologia as p_maior
            on t.maior_tecnologia_permitida = p_maior.tecnologia
        left join
            prioridade_tecnologia as p_menor
            on t.menor_tecnologia_permitida = p_menor.tecnologia
    ),
    viagem_autuacao_flags as (
        select
            vt.data,
            vt.id_viagem,
            logical_or(va.id_infracao = "017.III") as indicador_autuado_alterar_itinerario,
            logical_or(va.id_infracao = "023.X") as indicador_autuado_vista_inoperante,
            logical_or(
                va.id_infracao = "029.I"
            ) as indicador_autuado_nao_atender_parada,
            logical_or(
                va.id_infracao = "029.XIII"
            ) as indicador_autuado_iluminacao_insuficiente,
            logical_or(
                va.id_infracao = "040.I"
            ) as indicador_autuado_nao_concluir_itinerario
        from viagem_tecnologia vt
        left join
            veiculo_autuacao va
            on vt.data between va.data and date_add(va.data, interval 1 day)
            and vt.id_veiculo = va.id_veiculo
            and va.datetime_autuacao between vt.datetime_partida and vt.datetime_chegada
        group by 1, 2
    )
select
    vt.data,
    vt.id_viagem,
    vt.id_veiculo,
    vt.datetime_partida,
    vt.datetime_chegada,
    vt.modo,
    vt.placa,
    vt.ano_fabricacao,
    vt.tecnologia_apurada,
    vt.tecnologia_remunerada,
    vt.servico,
    vt.sentido,
    vt.distancia_planejada,
    vt.status = "Não licenciado" as indicador_nao_licenciado,
    vt.status = "Não vistoriado" as indicador_nao_vistoriado,
    vt.status = "Lacrado" as indicador_lacrado,
    vt.status = "Licenciado sem ar e não autuado"
    and vt.servico not in (select servico from {{ ref("servico_contrato_abreviado") }})
    and vt.data >= date("{{ var('DATA_SUBSIDIO_V19_INICIO') }}")
    as indicador_nao_autorizado_ausencia_ar,
    vt.indicador_penalidade_tecnologia
    and vt.data >= date('{{ var("DATA_SUBSIDIO_V16_INICIO") }}')
    as indicador_nao_autorizado_capacidade,
    vt.status = "Autuado por ar inoperante" as indicador_autuado_ar_inoperante,
    coalesce(af.indicador_autuado_alterar_itinerario, false)
    as indicador_autuado_alterar_itinerario,
    coalesce(af.indicador_autuado_vista_inoperante, false)
    as indicador_autuado_vista_inoperante,
    coalesce(af.indicador_autuado_nao_atender_parada, false)
    as indicador_autuado_nao_atender_parada,
    coalesce(af.indicador_autuado_iluminacao_insuficiente, false)
    as indicador_autuado_iluminacao_insuficiente,
    coalesce(af.indicador_autuado_nao_concluir_itinerario, false)
    as indicador_autuado_nao_concluir_itinerario,
    vt.status = "Registrado com ar inoperante" as indicador_registrado_ar_inoperante,
    json_set(
        json_set(
            vt.indicadores,
            '$.indicador_penalidade_tecnologia.valor',
            vt.indicador_penalidade_tecnologia
        ),
        '$.indicador_penalidade_tecnologia.data_inicio_vigencia',
        vt.data_inicio_vigencia
    ) as indicadores,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    "{{ var('version') }}" as versao,
    '{{ invocation_id }}' as id_execucao_dbt
from viagem_tecnologia vt
left join viagem_autuacao_flags af using (data, id_viagem)
where {{ incremental_filter }}
