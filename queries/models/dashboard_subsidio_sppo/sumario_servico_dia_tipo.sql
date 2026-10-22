{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    planejado as (
        select distinct
            data,
            tipo_dia,
            consorcio,
            servico,
            distancia_total_planejada as km_planejada
        from
            -- rj-smtr.projeto_subsidio_sppo.viagem_planejada
            {{ ref("viagem_planejada") }}
        where
            data >= date("{{ var('DATA_SUBSIDIO_V2_INICIO') }}")
            {% if is_incremental() %}
                and data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
            {% endif %}
            and (distancia_total_planejada > 0 or distancia_total_planejada is not null)
    ),
    viagem as (
        select data, servico, id_veiculo, id_viagem, tipo_viagem, distancia_planejada
        from
            -- rj-smtr.subsidio.viagem_transacao
            {{ ref("viagem_transacao") }}
        where
            data >= date("{{ var('DATA_SUBSIDIO_V2_INICIO') }}")
            {% if is_incremental() %}
                and data between date("{{ var('start_date') }}") and date(
                    "{{ var('end_date') }}"
                )
            {% endif %}
    ),
    servico_km_tipo as (
        select
            data,
            servico,
            tipo_viagem,
            count(id_viagem) as viagens,
            round(sum(distancia_planejada), 2) as km_apurada
        from viagem v
        group by 1, 2, 3
    ),
    servico_km_tipo_atualizado as (
        select
            * except (tipo_viagem),
            case
                when tipo_viagem = "Nao licenciado"
                then "Não licenciado"
                when tipo_viagem = "Licenciado com ar e autuado (023.II)"
                then "Autuado por ar inoperante"
                when tipo_viagem = "Licenciado sem ar"
                then "Licenciado sem ar e não autuado"
                when tipo_viagem = "Licenciado com ar e não autuado (023.II)"
                then "Licenciado com ar e não autuado"
                else tipo_viagem
            end as tipo_viagem
        from servico_km_tipo
    ),
    servico_km as (
        select
            p.data,
            p.tipo_dia,
            p.consorcio,
            p.servico,
            v.tipo_viagem,
            ifnull(v.viagens, 0) as viagens,
            ifnull(v.km_apurada, 0) as km_apurada,
        from planejado as p
        left join
            servico_km_tipo_atualizado v on p.data = v.data and p.servico = v.servico
    ),
    pivot_data as (
        select *
        from
            (
                select
                    data,
                    tipo_dia,
                    consorcio,
                    servico,
                    tipo_viagem,
                    viagens,
                    km_apurada,
                from servico_km
            ) pivot (
                sum(viagens) as viagens,
                sum(km_apurada) as km_apurada for tipo_viagem in (
                    "Registrado com ar inoperante" as registrado_com_ar_inoperante,
                    "Não licenciado" as n_licenciado,
                    "Autuado por ar inoperante" as autuado_ar_inoperante,
                    "Autuado por segurança" as autuado_seguranca,
                    "Autuado por limpeza/equipamento" as autuado_limpezaequipamento,
                    "Licenciado sem ar e não autuado" as licenciado_sem_ar_n_autuado,
                    "Licenciado com ar e não autuado" as licenciado_com_ar_n_autuado,
                    "Não vistoriado" as n_vistoriado,
                    "Sem transação" as sem_transacao
                )
            )
    )
select sd.*, pd.* except (data, tipo_dia, servico, consorcio)
from {{ ref("sumario_servico_dia") }} as sd
-- rj-smtr.dashboard_subsidio_sppo.sumario_servico_dia AS sd
left join pivot_data as pd on sd.data = pd.data and sd.servico = pd.servico
{% if is_incremental() %}
    where
        sd.data
        between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
{% endif %}
