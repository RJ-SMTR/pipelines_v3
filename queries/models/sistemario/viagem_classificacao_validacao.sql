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
  Fatos da viagem para openfisca_smtr.apurar (grão data × id_viagem).
  Inner join em servico_oferta_faixa só para carimbar faixa da partida
  (rn_faixa = 1). Viagem sem faixa não entra. Lote / programadas / I.2
  ficam no planejamento (`servico_oferta_faixa`) e em `operacao_lote`.

  Ainda stub: viagens incompletas / nao_apurada.
  Tipologia FCF (I.8 §4): tecnologia_fcf = coalesce(apurada, mínima);
  acima da máxima mantém apurada. tecnologia_remunerada só eco/auditoria.
  servico_tecnologia: via tecnologia_servico (menor permitida), não seed POR.
  Pressuposto: faixas em servico_oferta_faixa cobrem o dia sem gaps
  (partida sempre casa com alguma faixa por data×servico×sentido).
#}
{% set incremental_filter %}
    data between date('{{ var("date_range_start") }}') and date('{{ var("date_range_end") }}')
{% endset %}

with
    viagens as (
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
    -- inner join: pressuposto de cobertura contínua (sem gaps) em servico_oferta_faixa
    com_oferta as (
        select
            v.*,
            o.tipo_dia,
            o.faixa_horaria_inicio,
            o.faixa_horaria_fim,
            row_number() over (
                partition by v.data, v.id_viagem order by o.faixa_horaria_inicio
            ) as rn_faixa
        from viagens v
        inner join
            oferta o
            on o.data = v.data
            and o.servico = v.servico
            and o.sentido = v.sentido
            and v.datetime_partida
            between o.faixa_horaria_inicio and o.faixa_horaria_fim
    ),
    base as (select * except (rn_faixa) from com_oferta where rn_faixa = 1)
select
    b.data,
    b.id_viagem,
    b.datetime_partida,
    b.datetime_chegada,
    true as indicador_viagem_completa,
    coalesce(vk.indicador_validade, false) as indicador_viagem_valida,
    case
        when coalesce(vk.indicador_validade, false)
        then coalesce(vk.indicador_conformidade, false)
        else false
    end as indicador_viagem_conforme,
    cast(b.distancia_planejada as float64) as km_programada,
    cast(0 as float64) as km_percorrida,
    b.id_veiculo,
    b.placa,
    b.servico,
    b.sentido,
    format(
        '%02d:%02d',
        extract(hour from b.faixa_horaria_inicio),
        extract(minute from b.faixa_horaria_inicio)
    ) as faixa_horaria_inicio,
    format(
        '%02d:%02d',
        extract(hour from b.faixa_horaria_fim),
        extract(minute from b.faixa_horaria_fim)
    ) as faixa_horaria_fim,
    ts.menor_tecnologia_permitida as servico_tecnologia,
    ts.menor_tecnologia_permitida as servico_tecnologia_minima,
    ts.maior_tecnologia_permitida as servico_tecnologia_maxima,
    b.tipo_dia,
    b.tipo_viagem,
    b.indicadores,
    b.tecnologia_apurada,
    coalesce(b.tecnologia_apurada, ts.menor_tecnologia_permitida) as tecnologia_fcf,
    b.tecnologia_remunerada,
    b.indicador_regularidade_ar_condicionado_viagem,
    b.modo,
    b.ano_fabricacao,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from base b
left join
    tecnologia_servico ts
    on ts.servico = b.servico
    and (
        (b.data between ts.inicio_vigencia and ts.fim_vigencia)
        or (b.data >= ts.inicio_vigencia and ts.fim_vigencia is null)
    )
left join
    valor_km vk
    on b.tipo_viagem = vk.status
    and b.data >= vk.data_inicio
    and (vk.data_fim is null or b.data <= vk.data_fim)
