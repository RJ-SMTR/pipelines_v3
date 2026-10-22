-- Ticketing, one row per tap for one service date, cleaned for:
-- - fares that do not identify a rider (cash / botoeira)
-- - rows with no rider identifier
--
-- Source is chosen by the dbt variable ticketing_source:
-- - 'sample' (default): rj-smtr-sandbox.mit_ipea_project.transacao_drc_sample, an
-- anonymised Jaé extract with rounded timestamps and an H3 tile instead of
-- coordinates. The tap location is the tile centroid.
-- - 'jae': rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao, the production table.
-- The tap location is the validator's coordinates. hash_cliente carries a
-- BigQuery policy tag, so the caller needs Fine-Grained Reader on it.
-- Run with --vars '{"run_date": "YYYY-MM-DD", "ticketing_source": "jae"}'.
--
-- route_id is the GTFS route for the Jaé service. The service registry links most
-- services; those it has not linked (new services, SV and R variants) fall back to
-- the GTFS route whose short name equals the Jaé service name. route_source says
-- which path applied. direction_id is the Jaé sentido (0 = ida, 1 = volta), the
-- same convention as GTFS direction_id. vehicle_id is the 5-digit vehicle number,
-- which the GPS-based models match against right(gps.vehicle_id, 5).
{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "service_date",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

{% set ticketing_source = var("ticketing_source", "sample") %}

with
    {% if ticketing_source == "sample" %}
        source as (
            select
                t.data as service_date,
                extract(time from t.datetime_transacao_round) as boarding_time,
                t.datetime_transacao_round as tap_order,
                cast(t.hash_cliente_cartao_salt as string) as rider_id,
                t.produto as card_type,
                t.tipo_transacao_jae as transaction_type,
                lpad(cast(t.id_veiculo as string), 5, '0') as vehicle_id,
                cast(t.id_servico_jae as string) as service_id,
                t.sentido as direction_id,
                t.modo as mode,
                t.tile_id,
                h3.centroid as tap_location
            from `rj-smtr-sandbox.mit_ipea_project.transacao_drc_sample` as t
            left join {{ ref("vw_h3") }} as h3 on h3.tile_id = t.tile_id
            where
                t.data = "{{ var('run_date') }}"
                and t.hash_cliente_cartao_salt is not null
                -- Cash fares paid on board carry no rider identity
                and t.produto != 'Dinheiro (Botoeira)'
                and t.tipo_transacao_jae != 'Botoeira'
        ),
    {% elif ticketing_source == "jae" %}
        source as (
            select
                t.data as service_date,
                extract(time from t.datetime_transacao) as boarding_time,
                t.datetime_transacao as tap_order,
                t.hash_cliente as rider_id,
                t.tipo_pagamento as card_type,
                t.tipo_transacao as transaction_type,
                -- Keep the digits of the vehicle number; null for BRT in this table
                lpad(
                    right(regexp_replace(t.id_veiculo, r'[^0-9]', ''), 5), 5, '0'
                ) as vehicle_id,
                t.id_servico_jae as service_id,
                t.sentido as direction_id,
                t.modo as mode,
                cast(null as string) as tile_id,
                st_geogpoint(t.longitude, t.latitude) as tap_location
            from `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao` as t
            where
                t.data = "{{ var('run_date') }}"
                and t.hash_cliente is not null
                -- Cash fares paid on board carry no rider identity
                and t.tipo_transacao != 'Botoeira'
        ),
    {% else %}
        {{
            exceptions.raise_compiler_error(
                "ticketing_source must be 'sample' or 'jae', got '"
                ~ ticketing_source
                ~ "'"
            )
        }}
    {% endif %}

    registry as (
        -- Latest registry row per Jaé service id
        select id_servico_jae, id_servico_gtfs, servico_jae, servico
        from `rj-smtr.cadastro.servicos`
        where id_servico_jae is not null
        qualify
            row_number() over (
                partition by id_servico_jae order by data_inicio_vigencia desc
            )
            = 1
    ),

    gtfs_routes as (
        -- Latest feed's route per short name
        select route_short_name, route_id
        from `rj-smtr.gtfs.routes`
        qualify
            row_number() over (
                partition by route_short_name order by feed_start_date desc, route_id
            )
            = 1
    ),

    servicos as (
        select
            r.id_servico_jae,
            coalesce(r.id_servico_gtfs, g.route_id) as route_id,
            r.servico,
            case
                when r.id_servico_gtfs is not null
                then 'registry'
                when g.route_id is not null
                then 'short_name'
            end as route_source
        from registry as r
        left join gtfs_routes as g on g.route_short_name = r.servico_jae
    ),

    sequenced as (
        select
            t.service_date,
            t.boarding_time,
            t.rider_id,
            t.card_type,
            t.transaction_type,
            row_number() over (
                partition by t.rider_id, t.service_date order by t.tap_order
            ) as trip_sequence,
            t.vehicle_id,
            t.service_id,
            s.route_id,
            s.route_source,
            s.servico,
            t.direction_id,
            t.mode,
            t.tile_id,
            t.tap_location
        from source as t
        left join servicos as s on s.id_servico_jae = t.service_id
    )

select
    *,
    case
        when max(trip_sequence) over (partition by rider_id, service_date) = 1
        then 'only'
        when
            max(trip_sequence) over (partition by rider_id, service_date)
            = trip_sequence
        then 'last'
        when
            min(trip_sequence) over (partition by rider_id, service_date)
            = trip_sequence
        then 'first'
        else 'middle'
    end as chain_position
from sequenced
