{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["data", "servico"],
        incremental_strategy="insert_overwrite",
    )
}}

with
    -- Viagens planejadas (agrupadas por data e serviço)
    planejado as (
        select distinct
            data,
            tipo_dia,
            consorcio,
            servico,
            distancia_total_planejada as km_planejada
        from {{ ref("viagem_planejada") }}
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
            and (distancia_total_planejada > 0 or distancia_total_planejada is null)
    ),
    -- Viagens realizadas
    viagem as (
        select
            data,
            servico_realizado as servico,
            id_veiculo,
            id_viagem,
            distancia_planejada
        from {{ ref("viagem_completa") }}
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
    ),
    -- Apuração por de km
    servico_km_apuracao as (
        select
            p.data,
            p.tipo_dia,
            p.consorcio,
            p.servico,
            p.km_planejada,
            coalesce(count(v.id_viagem), 0) as viagens,
            coalesce(sum(v.distancia_planejada), 0) as km_apurada,
            coalesce(
                round(100 * sum(v.distancia_planejada) / p.km_planejada, 2), 0
            ) as perc_km_planejada
        from planejado as p
        left join viagem as v on p.data = v.data and p.servico = v.servico
        group by 1, 2, 3, 4, 5
    ),
    -- Status dos veículos
    veiculos as (
        select data, id_veiculo, status
        from {{ ref("sppo_veiculo_dia") }}
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
            and data < date("{{ var('DATA_SUBSIDIO_V3A_INICIO') }}")
    ),
    -- Km por tipo de viagem antes DATA_SUBSIDIO_V3A_INICIO
    servico_km_tipo as (
        select
            v.data,
            v.servico,
            case
                when
                    v.data >= date("{{ var('DATA_SUBSIDIO_V3_INICIO') }}")
                    and ve.status = "Nao licenciado"
                then "Não licenciado"
                when
                    v.data >= date("{{ var('DATA_SUBSIDIO_V3_INICIO') }}")
                    and ve.status = "Licenciado sem ar"
                then "Licenciado sem ar e não autuado"
                when
                    v.data >= date("{{ var('DATA_SUBSIDIO_V3_INICIO') }}")
                    and ve.status = "Licenciado com ar e não autuado (023.II)"
                then "Licenciado com ar e não autuado"
                else ve.status
            end as tipo_viagem,
            count(id_viagem) as viagens,
            sum(distancia_planejada) as km_apurada
        from viagem as v
        left join veiculos as ve on ve.data = v.data and ve.id_veiculo = v.id_veiculo
        where v.data < date("{{ var('DATA_SUBSIDIO_V3A_INICIO') }}")
        group by 1, 2, 3
    ),
    -- Subsídio por tipo de viagem antes DATA_SUBSIDIO_V3A_INICIO
    subsidio_km_tipo as (
        select distinct
            v.*, round(v.km_apurada * sp.subsidio_km, 2) as valor_subsidio_apurado
        from servico_km_tipo v
        left join
            {{ ref("subsidio_parametros") }} as sp
            on v.data between sp.data_inicio and sp.data_fim
            and v.tipo_viagem = sp.status
    ),
    -- Subsídio por serviço antes DATA_SUBSIDIO_V3A_INICIO
    subsidio_pre_v3a as (
        select data, servico, sum(valor_subsidio_apurado) as valor_subsidio_apurado
        from subsidio_km_tipo
        group by 1, 2
    ),
    -- Viagens remuneradas
    viagens_remuneradas as (
        select data, servico, distancia_planejada, subsidio_km
        from {{ ref("viagens_remuneradas") }}
        where
            data
            between date("{{ var('start_date') }}") and date("{{ var('end_date') }}")
            and data >= date("{{ var('DATA_SUBSIDIO_V3A_INICIO') }}")
            and indicador_viagem_dentro_limite is true
    ),
    -- Subsídio pós DATA_SUBSIDIO_V3A_INICIO
    subsidio_pos_v3a as (
        select
            data,
            servico,
            sum(distancia_planejada * subsidio_km) as valor_subsidio_apurado
        from viagens_remuneradas
        group by 1, 2
    ),
    -- União das duas lógicas de subsídio
    subsidio_unificado as (
        select data, servico, valor_subsidio_apurado
        from subsidio_pre_v3a

        union all

        select data, servico, valor_subsidio_apurado
        from subsidio_pos_v3a
    )
select
    s.*,
    if(
        p.valor is null, coalesce(su.valor_subsidio_apurado, 0), 0
    ) as valor_subsidio_pago,
    ifnull(- p.valor, 0) as valor_penalidade
from servico_km_apuracao s
left join
    {{ ref("valor_tipo_penalidade") }} as p
    on s.data between p.data_inicio and p.data_fim
    and s.perc_km_planejada >= p.perc_km_inferior
    and s.perc_km_planejada < p.perc_km_superior
left join subsidio_unificado as su on s.data = su.data and s.servico = su.servico
