{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        incremental_strategy="insert_overwrite",
    )
}}

{% set aux_viagem_status = ref("aux_viagem_status") %}
{% if execute and is_incremental() %}
    {% set modified_partitions = get_modified_partitions_filter(
        aux_viagem_status,
        truncate_date=true,
        max_age_days=var("viagem_validacao_max_age_days", 6),
    ) %}
{% else %} {% set modified_partitions = [] %}
{% endif %}

{% set incremental_filter %}
    (
        data between date('{{ var("date_range_start") }}') and date(
            '{{ var("date_range_end") }}'
        )
        {% if is_incremental() and modified_partitions | length > 0 %}
            or data in ({{ modified_partitions | join(", ") }})
        {% endif %}
    )
{% endset %}

with
    viagem_indicador as (
        select *
        from {{ ref("aux_viagem_indicador") }}
        where {{ incremental_filter }} and sistema = "RIO"
    )
select
    concat(
        format_date('%Y-%m-%d', c.data),
        '|',
        c.lote,
        '|',
        c.id_veiculo,
        '|',
        format_datetime('%Y-%m-%dT%H:%M:%S', current_datetime("America/Sao_Paulo"))
    ) as id_apuracao,
    c.data,
    c.id_viagem,
    c.datetime_partida,
    c.datetime_chegada,
    ifnull(c.tipo_execucao_viagem, "COMPLETA")
    != "INCOMPLETA" as indicador_viagem_completa,
    not (
        c.indicador_validador_associado_incorretamente
        or c.indicador_autuado_vista_inoperante
        or c.indicador_nao_licenciado
        or c.indicador_nao_vistoriado
        or c.indicador_lacrado
        or c.indicador_sem_transacao_tipo
        or c.indicador_validador_fechado
        or c.indicador_autuado_nao_atender_parada
        or c.indicador_autuado_alterar_itinerario
        or c.indicador_autuado_nao_concluir_itinerario
    ) as indicador_viagem_valida,
    not (
        c.indicador_validador_associado_incorretamente
        or c.indicador_autuado_vista_inoperante
        or c.indicador_nao_licenciado
        or c.indicador_nao_vistoriado
        or c.indicador_lacrado
        or c.indicador_sem_transacao_tipo
        or c.indicador_validador_fechado
        or c.indicador_autuado_nao_atender_parada
        or c.indicador_autuado_alterar_itinerario
        or c.indicador_autuado_nao_concluir_itinerario
        or c.indicador_nao_autorizado_capacidade
        or c.indicador_autuado_ar_inoperante
        or c.indicador_registrado_ar_inoperante
        or c.indicador_detectado_ar_inoperante
    ) as indicador_viagem_conforme,
    c.indicador_viagem_apurada,
    c.km_programada,
    c.km_percorrida,
    c.id_veiculo,
    c.placa,
    c.servico,
    c.sentido,
    c.shape_id,
    c.faixa_horaria_inicio,
    c.faixa_horaria_fim,
    c.menor_tecnologia_permitida as tecnologia_minima_servico,
    c.maior_tecnologia_permitida as tecnologia_maxima_servico,
    coalesce(c.tipo_dia_oferta, c.tipo_dia) as tipo_dia,
    json_object(
        'indicador_validador_associado_incorretamente',
        json_object('valor', c.indicador_validador_associado_incorretamente),
        'indicador_autuado_vista_inoperante',
        json_object('valor', c.indicador_autuado_vista_inoperante),
        'indicador_nao_licenciado',
        json_object('valor', c.indicador_nao_licenciado),
        'indicador_nao_vistoriado',
        json_object('valor', c.indicador_nao_vistoriado),
        'indicador_lacrado',
        json_object('valor', c.indicador_lacrado),
        'indicador_sem_transacao_tipo',
        json_object('valor', c.indicador_sem_transacao_tipo),
        'indicador_validador_fechado',
        json_object('valor', c.indicador_validador_fechado),
        'indicador_autuado_nao_atender_parada',
        json_object('valor', c.indicador_autuado_nao_atender_parada),
        'indicador_autuado_alterar_itinerario',
        json_object('valor', c.indicador_autuado_alterar_itinerario),
        'indicador_autuado_nao_concluir_itinerario',
        json_object('valor', c.indicador_autuado_nao_concluir_itinerario),
        'indicador_nao_autorizado_capacidade',
        json_object('valor', c.indicador_nao_autorizado_capacidade),
        'indicador_autuado_ar_inoperante',
        json_object('valor', c.indicador_autuado_ar_inoperante),
        'indicador_registrado_ar_inoperante',
        json_object('valor', c.indicador_registrado_ar_inoperante),
        'indicador_detectado_ar_inoperante',
        json_object('valor', c.indicador_detectado_ar_inoperante),
        'indicador_regularidade_ar_condicionado_viagem',
        json_object('valor', c.indicador_regularidade_ar_condicionado_viagem)
    ) as indicadores,
    c.tecnologia_apurada,
    coalesce(c.tecnologia_apurada, c.menor_tecnologia_permitida) as tecnologia_fcf,
    c.tecnologia_remunerada,
    c.indicador_regularidade_ar_condicionado_viagem,
    c.modo,
    c.ano_fabricacao,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from viagem_indicador as c
