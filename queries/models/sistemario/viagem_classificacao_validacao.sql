{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
        on_schema_change="append_new_columns",
        tags=["remuneracao", "openfisca", "wip"],
    )
}}

{#
  Fan-in RIO: viagem_valida + veículo + temperatura + bilhetagem.
  Tab. 2 (valida / conforme) a partir das flags vs valor_km (dicionário).
  Sem join em um tipo_viagem. Sem inner join de oferta (faixa só carimbo).
  Stub: indicador_viagem_completa = true; km_percorrida = 0;
  indicador_dentro_do_teto_programado = true.
#}
{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

with
    viagens as (
        select
            data,
            id_viagem,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            placa,
            ano_fabricacao,
            servico,
            sentido,
            distancia_planejada,
            modo,
            tipo_dia,
            tecnologia_apurada
        from {{ ref("viagem_valida") }}
        where {{ incremental_filter }}
    ),
    status_veiculo as (
        select *
        from {{ ref("aux_veiculo_status_viagem") }}
        where {{ incremental_filter }}
    ),
    temperatura as (
        select *
        from {{ ref("viagem_valida_temperatura") }}
        where {{ incremental_filter }}
    ),
    bilhetagem as (
        select *
        from {{ ref("viagem_valida_bilhetagem") }}
        where {{ incremental_filter }}
    ),
    oferta as (
        select data, tipo_dia, servico, sentido, faixa_horaria_inicio, faixa_horaria_fim
        from {{ ref("servico_oferta_faixa") }}
        where {{ incremental_filter }}
    ),
    tecnologia_servico as (
        select
            servico,
            inicio_vigencia,
            fim_vigencia,
            menor_tecnologia_permitida,
            maior_tecnologia_permitida
        from {{ ref("tecnologia_servico") }}
    ),
    valor_km as (
        select distinct
            data_inicio, data_fim, status, indicador_validade, indicador_conformidade
        from {{ ref("valor_km_tipo_viagem") }}
    ),
    base as (
        select
            v.data,
            v.id_viagem,
            v.datetime_partida,
            v.datetime_chegada,
            v.id_veiculo,
            coalesce(s.placa, v.placa) as placa,
            coalesce(s.ano_fabricacao, v.ano_fabricacao) as ano_fabricacao,
            v.servico,
            v.sentido,
            v.distancia_planejada,
            v.modo,
            v.tipo_dia,
            coalesce(
                s.tecnologia_apurada, t.tecnologia_apurada, v.tecnologia_apurada
            ) as tecnologia_apurada,
            coalesce(
                s.tecnologia_remunerada, t.tecnologia_remunerada
            ) as tecnologia_remunerada,
            coalesce(s.indicador_nao_licenciado, false) as indicador_nao_licenciado,
            coalesce(s.indicador_nao_vistoriado, false) as indicador_nao_vistoriado,
            coalesce(s.indicador_lacrado, false) as indicador_lacrado,
            coalesce(
                s.indicador_nao_autorizado_ausencia_ar, false
            ) as indicador_nao_autorizado_ausencia_ar,
            coalesce(
                s.indicador_nao_autorizado_capacidade, false
            ) as indicador_nao_autorizado_capacidade,
            coalesce(
                s.indicador_autuado_ar_inoperante, false
            ) as indicador_autuado_ar_inoperante,
            coalesce(
                s.indicador_autuado_alterar_itinerario, false
            ) as indicador_autuado_alterar_itinerario,
            coalesce(
                s.indicador_autuado_vista_inoperante, false
            ) as indicador_autuado_vista_inoperante,
            coalesce(
                s.indicador_autuado_nao_atender_parada, false
            ) as indicador_autuado_nao_atender_parada,
            coalesce(
                s.indicador_autuado_iluminacao_insuficiente, false
            ) as indicador_autuado_iluminacao_insuficiente,
            coalesce(
                s.indicador_autuado_nao_concluir_itinerario, false
            ) as indicador_autuado_nao_concluir_itinerario,
            coalesce(
                s.indicador_registrado_ar_inoperante, false
            ) as indicador_registrado_ar_inoperante,
            coalesce(
                t.indicador_detectado_ar_inoperante, false
            ) as indicador_detectado_ar_inoperante,
            coalesce(
                t.indicador_regularidade_ar_condicionado_viagem, true
            ) as indicador_regularidade_ar_condicionado_viagem,
            coalesce(
                b.indicador_sem_transacao_tipo, false
            ) as indicador_sem_transacao_tipo,
            coalesce(
                b.indicador_validador_fechado, false
            ) as indicador_validador_fechado,
            coalesce(
                b.indicador_validador_associado_incorretamente, false
            ) as indicador_validador_associado_incorretamente,
            coalesce(b.indicadores, t.indicadores, s.indicadores) as indicadores
        from viagens v
        left join status_veiculo s using (data, id_viagem)
        left join temperatura t using (data, id_viagem)
        left join bilhetagem b using (data, id_viagem)
    ),
    flag_status as (
        select b.data, b.id_viagem, f.status
        from base b
        cross join
            unnest(
                [
                    struct(
                        b.indicador_nao_licenciado as flag, "Não licenciado" as status
                    ),
                    struct(b.indicador_nao_vistoriado, "Não vistoriado"),
                    struct(b.indicador_lacrado, "Lacrado"),
                    struct(
                        b.indicador_nao_autorizado_ausencia_ar,
                        "Não autorizado por ausência de ar-condicionado"
                    ),
                    struct(
                        b.indicador_nao_autorizado_capacidade,
                        "Não autorizado por capacidade"
                    ),
                    struct(
                        b.indicador_autuado_ar_inoperante, "Autuado por ar inoperante"
                    ),
                    struct(
                        b.indicador_autuado_alterar_itinerario,
                        "Autuado por alterar itinerário"
                    ),
                    struct(
                        b.indicador_autuado_vista_inoperante,
                        "Autuado por vista inoperante"
                    ),
                    struct(
                        b.indicador_autuado_nao_atender_parada,
                        "Autuado por não atender solicitação de parada"
                    ),
                    struct(
                        b.indicador_autuado_iluminacao_insuficiente,
                        "Autuado por iluminação insuficiente"
                    ),
                    struct(
                        b.indicador_autuado_nao_concluir_itinerario,
                        "Autuado por não concluir itinerário"
                    ),
                    struct(
                        b.indicador_registrado_ar_inoperante,
                        "Registrado com ar inoperante"
                    ),
                    struct(
                        b.indicador_detectado_ar_inoperante,
                        "Detectado com ar inoperante"
                    ),
                    struct(b.indicador_sem_transacao_tipo, "Sem transação"),
                    struct(b.indicador_validador_fechado, "Validador fechado"),
                    struct(
                        b.indicador_validador_associado_incorretamente,
                        "Validador associado incorretamente"
                    )
                ]
            ) as f
        where f.flag
    ),
    fechamento as (
        select
            fs.data,
            fs.id_viagem,
            logical_and(
                coalesce(vk.indicador_validade, true)
            ) as indicador_viagem_valida,
            logical_and(
                coalesce(vk.indicador_conformidade, true)
            ) as indicador_viagem_conforme_flags
        from flag_status fs
        left join
            valor_km vk
            on fs.status = vk.status
            and fs.data >= vk.data_inicio
            and (vk.data_fim is null or fs.data <= vk.data_fim)
        group by 1, 2
    ),
    com_oferta as (
        select
            b.*,
            o.tipo_dia as tipo_dia_oferta,
            o.faixa_horaria_inicio,
            o.faixa_horaria_fim,
            row_number() over (
                partition by b.data, b.id_viagem order by o.faixa_horaria_inicio
            ) as rn_faixa
        from base b
        left join
            oferta o
            on o.data = b.data
            and o.servico = b.servico
            and o.sentido = b.sentido
            and b.datetime_partida
            between o.faixa_horaria_inicio and o.faixa_horaria_fim
    ),
    carimbo as (select * except (rn_faixa) from com_oferta where rn_faixa = 1)
select
    c.data,
    c.id_viagem,
    c.datetime_partida,
    c.datetime_chegada,
    true as indicador_viagem_completa,
    true as indicador_dentro_do_teto_programado,
    coalesce(f.indicador_viagem_valida, true) as indicador_viagem_valida,
    case
        when coalesce(f.indicador_viagem_valida, true)
        then coalesce(f.indicador_viagem_conforme_flags, true)
        else false
    end as indicador_viagem_conforme,
    cast(c.distancia_planejada as float64) as km_programada,
    cast(0 as float64) as km_percorrida,
    c.id_veiculo,
    c.placa,
    c.servico,
    c.sentido,
    format(
        '%02d:%02d',
        extract(hour from c.faixa_horaria_inicio),
        extract(minute from c.faixa_horaria_inicio)
    ) as faixa_horaria_inicio,
    format(
        '%02d:%02d',
        extract(hour from c.faixa_horaria_fim),
        extract(minute from c.faixa_horaria_fim)
    ) as faixa_horaria_fim,
    ts.menor_tecnologia_permitida as servico_tecnologia,
    ts.menor_tecnologia_permitida as servico_tecnologia_minima,
    ts.maior_tecnologia_permitida as servico_tecnologia_maxima,
    coalesce(c.tipo_dia_oferta, c.tipo_dia) as tipo_dia,
    c.indicadores,
    c.tecnologia_apurada,
    coalesce(c.tecnologia_apurada, ts.menor_tecnologia_permitida) as tecnologia_fcf,
    c.tecnologia_remunerada,
    c.indicador_regularidade_ar_condicionado_viagem,
    c.modo,
    c.ano_fabricacao,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from carimbo c
left join fechamento f using (data, id_viagem)
left join
    tecnologia_servico ts
    on ts.servico = c.servico
    and (
        (c.data between ts.inicio_vigencia and ts.fim_vigencia)
        or (c.data >= ts.inicio_vigencia and ts.fim_vigencia is null)
    )
