{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

{% set viagem_valida = ref("viagem_valida") %}
{% if execute and is_incremental() %}
    {% set modified_partitions = get_modified_partitions_filter(
        viagem_valida,
        truncate_date=true,
        max_age_days=var("viagem_validacao_max_age_days", 6),
    ) %}
{% else %} {% set modified_partitions = [] %}
{% endif %}

{% set incremental_filter %}
    {% if var("flow_name") == "treatment--monitoramento-temperatura" %}
        (
            data between date("{{ var('date_range_start') }}") and date(
                "{{ var('date_range_end') }}"
            )
            {% if is_incremental() %}
                and data < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
            {% endif %}
            {% if is_incremental() and modified_partitions | length > 0 %}
                or (
                    data in ({{ modified_partitions | join(", ") }})
                    and data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
                )
            {% endif %}
        )
        and data >= date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
    {% else %}
        (
            data between date("{{ var('date_range_start') }}") and date(
                "{{ var('date_range_end') }}"
            )
            {% if is_incremental() and modified_partitions | length > 0 %}
                or (
                    data in ({{ modified_partitions | join(", ") }})
                    and data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
                )
            {% endif %}
        )
        and data >= date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")
    {% endif %}
{% endset %}

with
    viagens as (
        select
            data,
            servico_realizado as servico,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            id_viagem,
            distancia_planejada,
            sentido,
            "Ônibus" as modo,
            cast(null as string) as shape_id,
            cast(null as string) as tipo_dia,
            cast(null as string) as tipo_execucao_viagem,
            cast(null as float64) as km_percorrida_gps,
            "SPPO" as sistema
        from {{ ref("viagem_completa") }}
        where
            {{ incremental_filter }}
            and data < date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")

        union all

        select
            data,
            servico,
            datetime_partida,
            datetime_chegada,
            id_veiculo,
            id_viagem,
            distancia_planejada,
            sentido,
            modo,
            shape_id,
            tipo_dia,
            tipo_execucao_viagem,
            km_percorrida_gps,
            sistema
        from {{ viagem_valida }}
        where
            {{ incremental_filter }}
            and data >= date("{{ var('DATA_SUBSIDIO_V25_INICIO') }}")
            and sistema in ("RIO", "SPPO")
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
    ordem_status as (
        select distinct data_inicio, data_fim, status, ordem
        from {{ ref("valor_km_tipo_viagem") }}
    ),
    oferta as (
        select
            data,
            servico,
            sentido,
            faixa_horaria_inicio,
            faixa_horaria_fim,
            tipo_dia,
            lote
        from {{ ref("servico_oferta_faixa") }}
        where {{ incremental_filter }}
    ),
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
            case
                when v.sistema = "SPPO" then substr(v.id_veiculo, 2) else v.id_veiculo
            end as id_veiculo_join,
            ve.placa,
            ve.ano_fabricacao,
            v.id_viagem,
            v.distancia_planejada,
            v.sentido,
            v.modo,
            v.shape_id,
            v.tipo_dia,
            v.tipo_execucao_viagem,
            v.km_percorrida_gps,
            v.sistema,
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
            vs.id_veiculo_join,
            vs.placa,
            vs.ano_fabricacao,
            vs.id_viagem,
            vs.distancia_planejada,
            vs.sentido,
            vs.modo,
            vs.shape_id,
            vs.tipo_dia,
            vs.tipo_execucao_viagem,
            vs.km_percorrida_gps,
            vs.sistema,
            vs.status,
            vs.indicadores,
            t.inicio_vigencia as data_inicio_vigencia,
            t.menor_tecnologia_permitida,
            t.maior_tecnologia_permitida,
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
    viagem_autuacao as (
        select
            vt.data,
            vt.id_viagem,
            logical_or(
                va.id_infracao = "017.III"
            ) as indicador_autuado_alterar_itinerario,
            logical_or(va.id_infracao = "023.X") as indicador_autuado_vista_inoperante,
            logical_or(
                va.id_infracao = "029.I"
            ) as indicador_autuado_nao_atender_parada,
            logical_or(
                va.id_infracao = "029.XIII"
            ) as indicador_autuado_iluminacao_insuficiente,
            logical_or(
                va.id_infracao = "040.I"
            ) as indicador_autuado_nao_concluir_itinerario,
            array_agg(distinct va.id_infracao ignore nulls) as ids_infracao
        from viagem_tecnologia vt
        left join
            veiculo_autuacao va
            on vt.data between va.data and date_add(va.data, interval 1 day)
            and vt.id_veiculo = va.id_veiculo
            and va.datetime_autuacao between vt.datetime_partida and vt.datetime_chegada
        group by 1, 2
    ),
    viagem_tipo as (
        select
            vt.data,
            vt.id_viagem,
            case
                when vt.status = "Não licenciado"
                then "Não licenciado"
                when vt.status = "Não vistoriado"
                then "Não vistoriado"
                when vt.status = "Lacrado"
                then "Lacrado"
                when
                    vt.status = "Licenciado sem ar e não autuado"
                    and vt.servico
                    not in (select servico from {{ ref("servico_contrato_abreviado") }})
                    and vt.data >= date("{{ var('DATA_SUBSIDIO_V19_INICIO') }}")
                then "Não autorizado por ausência de ar-condicionado"
                when
                    vt.indicador_penalidade_tecnologia
                    and vt.data >= date('{{ var("DATA_SUBSIDIO_V16_INICIO") }}')
                then "Não autorizado por capacidade"
                when vt.status = "Autuado por ar inoperante"
                then "Autuado por ar inoperante"
                when id_infracao = "017.III"
                then "Autuado por alterar itinerário"
                when id_infracao = "023.X"
                then "Autuado por vista inoperante"
                when id_infracao = "029.I"
                then "Autuado por não atender solicitação de parada"
                when id_infracao = "029.XIII"
                then "Autuado por iluminação insuficiente"
                when id_infracao = "040.I"
                then "Autuado por não concluir itinerário"
                when vt.status = "Registrado com ar inoperante"
                then "Registrado com ar inoperante"
                when vt.status = "Licenciado sem ar e não autuado"
                then "Licenciado sem ar e não autuado"
                when vt.status = "Licenciado com ar e não autuado"
                then "Licenciado com ar e não autuado"
            end as tipo_viagem
        from viagem_tecnologia vt
        left join viagem_autuacao va using (data, id_viagem)
        left join
            unnest(
                if(
                    va.ids_infracao is null or array_length(va.ids_infracao) = 0,
                    [cast(null as string)],
                    va.ids_infracao
                )
            ) as id_infracao
    ),
    tipo_priorizado as (
        select vt.data, vt.id_viagem, vt.tipo_viagem
        from viagem_tipo vt
        left join
            ordem_status os
            on vt.data between os.data_inicio and os.data_fim
            and vt.tipo_viagem = os.status
        qualify
            row_number() over (partition by vt.data, vt.id_viagem order by os.ordem) = 1
    ),
    oferta_viagem as (
        select
            vt.data,
            vt.id_viagem,
            o.lote,
            o.faixa_horaria_inicio,
            o.faixa_horaria_fim,
            o.tipo_dia as tipo_dia_oferta
        from viagem_tecnologia vt
        left join
            oferta o
            on o.data = vt.data
            and o.servico = vt.servico
            and o.sentido = vt.sentido
            and vt.datetime_partida
            between o.faixa_horaria_inicio and o.faixa_horaria_fim
        qualify
            row_number() over (
                partition by vt.data, vt.id_viagem order by o.faixa_horaria_inicio
            )
            = 1
    )
select
    vt.data,
    vt.id_viagem,
    vt.id_veiculo,
    vt.id_veiculo_join,
    vt.datetime_partida,
    vt.datetime_chegada,
    vt.modo,
    vt.placa,
    vt.ano_fabricacao,
    vt.tecnologia_apurada,
    vt.tecnologia_remunerada,
    vt.menor_tecnologia_permitida,
    vt.maior_tecnologia_permitida,
    vt.servico,
    vt.sentido,
    vt.distancia_planejada,
    vt.shape_id,
    vt.tipo_dia,
    ov.tipo_dia_oferta,
    vt.tipo_execucao_viagem,
    vt.km_percorrida_gps,
    vt.sistema,
    tp.tipo_viagem,
    vt.status = "Não licenciado" as indicador_nao_licenciado,
    vt.status = "Não vistoriado" as indicador_nao_vistoriado,
    vt.status = "Lacrado" as indicador_lacrado,
    vt.indicador_penalidade_tecnologia
    and vt.data >= date(
        '{{ var("DATA_SUBSIDIO_V16_INICIO") }}'
    ) as indicador_nao_autorizado_capacidade,
    vt.status = "Autuado por ar inoperante" as indicador_autuado_ar_inoperante,
    coalesce(
        va.indicador_autuado_alterar_itinerario, false
    ) as indicador_autuado_alterar_itinerario,
    coalesce(
        va.indicador_autuado_vista_inoperante, false
    ) as indicador_autuado_vista_inoperante,
    coalesce(
        va.indicador_autuado_nao_atender_parada, false
    ) as indicador_autuado_nao_atender_parada,
    coalesce(
        va.indicador_autuado_iluminacao_insuficiente, false
    ) as indicador_autuado_iluminacao_insuficiente,
    coalesce(
        va.indicador_autuado_nao_concluir_itinerario, false
    ) as indicador_autuado_nao_concluir_itinerario,
    vt.status = "Registrado com ar inoperante" as indicador_registrado_ar_inoperante,
    ov.lote,
    ov.faixa_horaria_inicio,
    ov.faixa_horaria_fim,
    ov.faixa_horaria_inicio is not null as indicador_viagem_apurada,
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
left join viagem_autuacao va using (data, id_viagem)
left join tipo_priorizado tp using (data, id_viagem)
left join oferta_viagem ov using (data, id_viagem)
where {{ incremental_filter }}
