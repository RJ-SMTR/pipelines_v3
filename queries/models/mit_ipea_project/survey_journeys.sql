-- One row per journey for the alighting-stop survey.
--
-- A journey is a run of taps by the same rider on the same day linked by fare
-- integration: a tap whose transaction_type starts with 'Integração' continues the
-- previous journey, any other tap starts a new one. A gap of more than
-- max_transfer_gap_minutes between taps also starts a new journey, as a guard.
-- The survey asks about the final leg's alighting stop, so a journey is included
-- only when that leg is a bus tap with a predicted alighting stop and a full set of
-- four options in origin_destination_stop_candidates. Predictions of every
-- confidence tier are kept, including those beyond the walking cap, so the sample
-- can be stratified by confidence_tier; filter on it when drawing a batch.
-- stop_id_1..4 are the options in display order; prediction_slot says which is the
-- model's prediction and is for analysis only, not for the message.
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

{% set max_transfer_gap_minutes = 180 %}

{% set feed_info = ref("feed_info_gtfs") %}

{% if execute and is_incremental() %}
    {% set columns = (
        list_columns()
        | reject(
            "in",
            [
                "journey_key",
                "versao",
                "datetime_ultima_atualizacao",
                "id_execucao_dbt",
            ],
        )
        | list
    ) %}
    {% set sha_column %}
        sha256(
            to_json_string(
                struct(
                    {% for c in columns %}
                        {% if c == "rider_id" %}
                            to_base64({{ c }}) as `{{ c }}`
                        {% else %}
                            cast({{ c }} as string) as `{{ c }}`
                        {% endif %}
                        {% if not loop.last %}, {% endif %}
                    {% endfor %}
                )
            )
        )
    {% endset %}

    {% set gtfs_feeds_query %}
        select concat("'", feed_start_date, "'") as feed_start_date
        from {{ feed_info }}
        where
            feed_start_date <= date("{{ var('date_range_end') }}")
            and (feed_end_date IS NULL OR feed_end_date >= date("{{ var('date_range_start') }}"))
    {% endset %}

    {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}

{% else %}
    {% set sha_column %}
        cast(null as bytes)
    {% endset %}
{% endif %}

with
    taps as (
        select
            rider_id,
            service_date,
            trip_sequence,
            boarding_time,
            transaction_type,
            mode,
            servico,
            descricao_servico,
            tap_location,
            lag(boarding_time) over (
                partition by rider_id, service_date order by trip_sequence
            ) as previous_boarding_time
        from {{ ref("aux_ticketing") }}
        where
            {% if is_incremental() %}
                service_date between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
            {% else %} service_date >= '{{ var("mit_ipea_project_initial_date") }}'

            {% endif %}
    ),
    journey_flags as (
        select
            *,
            case
                when previous_boarding_time is null
                then 1
                when transaction_type not like 'Integração%'
                then 1
                when
                    time_diff(boarding_time, previous_boarding_time, minute)
                    > {{ max_transfer_gap_minutes }}
                then 1
                else 0
            end as new_journey
        from taps
    ),
    legs as (
        select
            *,
            sum(new_journey) over (
                partition by rider_id, service_date order by trip_sequence
            ) as journey_id
        from journey_flags
    ),
    journeys as (
        select
            rider_id,
            service_date,
            journey_id,
            min(trip_sequence) as first_trip_sequence,
            max(trip_sequence) as final_trip_sequence,
            count(*) - 1 as n_transfers
        from legs
        group by rider_id, service_date, journey_id
    ),
    -- Final-leg options pivoted into display order
    options as (
        select
            rider_id,
            service_date,
            trip_sequence,
            count(*) as n_options,
            max(if(option_role = 'prediction', display_order, null)) as prediction_slot,
            max(if(display_order = 1, stop_id, null)) as stop_id_1,
            max(if(display_order = 2, stop_id, null)) as stop_id_2,
            max(if(display_order = 3, stop_id, null)) as stop_id_3,
            max(if(display_order = 4, stop_id, null)) as stop_id_4,
            max(if(display_order = 1, stop_name, null)) as stop_name_1,
            max(if(display_order = 2, stop_name, null)) as stop_name_2,
            max(if(display_order = 3, stop_name, null)) as stop_name_3,
            max(if(display_order = 4, stop_name, null)) as stop_name_4
        from {{ ref("aux_origin_destination_stop_candidates") }}
        where
            {% if is_incremental() %}
                service_date between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
            {% else %} service_date >= '{{ var("mit_ipea_project_initial_date") }}'

            {% endif %}
        group by rider_id, service_date, trip_sequence
    ),
    stops as (
        select
            feed_start_date, feed_end_date, stop_id, any_value(stop_name) as stop_name
        from {{ ref("aux_vw_stops") }}
        where
            {% if gtfs_feeds | length > 0 %}
                feed_start_date in ({{ gtfs_feeds | join(", ") }})
            {% else %} false
            {% endif %}
        group by all
    ),
    origin_destination_stop as (
        select *
        from {{ ref("aux_origin_destination_stop") }}
        where
            {% if is_incremental() %}
                service_date between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
            {% else %} service_date >= '{{ var("mit_ipea_project_initial_date") }}'

            {% endif %}
    ),
    new_data as (
        select
            j.rider_id,
            j.service_date,
            c.documento as rider_document,
            j.first_trip_sequence,
            j.final_trip_sequence,
            j.n_transfers,
            first_leg.boarding_time,
            first_leg.mode as boarding_mode,
            first_leg.servico as boarding_servico,
            first_leg.descricao_servico as boarding_descricao_servico,
            first_leg.tap_location as boarding_tap_location,
            first_od.boarding_stop_id,
            boarding_stop.stop_name as boarding_stop_name,
            final_leg.boarding_time as final_leg_boarding_time,
            final_leg.servico as alighting_servico,
            final_od.alighting_time,
            final_od.alighting_to_next_boarding_distance_m as predicted_walk_distance_m,
            final_od.runner_up_walk_distance_m,
            final_od.alighting_within_walk_cap,
            final_od.confidence_tier,
            final_od.direction_source,
            o.stop_id_1,
            o.stop_id_2,
            o.stop_id_3,
            o.stop_id_4,
            o.stop_name_1,
            o.stop_name_2,
            o.stop_name_3,
            o.stop_name_4,
            o.prediction_slot
        from journeys as j
        join
            legs as first_leg
            on first_leg.rider_id = j.rider_id
            and first_leg.service_date = j.service_date
            and first_leg.trip_sequence = j.first_trip_sequence
        join
            legs as final_leg
            on final_leg.rider_id = j.rider_id
            and final_leg.service_date = j.service_date
            and final_leg.trip_sequence = j.final_trip_sequence
        left join
            origin_destination_stop as first_od
            on first_od.rider_id = j.rider_id
            and first_od.service_date = j.service_date
            and first_od.trip_sequence = j.first_trip_sequence
        join
            origin_destination_stop as final_od
            on final_od.rider_id = j.rider_id
            and final_od.service_date = j.service_date
            and final_od.trip_sequence = j.final_trip_sequence
        join
            options as o
            on o.rider_id = j.rider_id
            and o.service_date = j.service_date
            and o.trip_sequence = j.final_trip_sequence
        left join
            stops as boarding_stop
            on boarding_stop.stop_id = first_od.boarding_stop_id
            and j.service_date >= boarding_stop.feed_start_date
            and (
                j.service_date <= boarding_stop.feed_end_date
                or boarding_stop.feed_end_date is null
            )
        join {{ ref("cliente_jae") }} c on j.rider_id = sha256(c.id_cliente)
        where
            final_leg.mode = 'Ônibus'
            and final_od.alighting_stop_id is not null
            and o.n_options = 4
    ),
    {% if is_incremental() %}
        current_data as (
            select *
            from {{ this }}
            where
                service_date between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )

        ),
    {% endif %}
    new_data_sha as (select *, {{ sha_column }} as new_sha from new_data),
    current_data_sha as (
        {% if is_incremental() %}

            select
                journey_key,
                rider_id,
                service_date,
                boarding_time,
                boarding_servico,
                {{ sha_column }} as current_sha,
                datetime_ultima_atualizacao as current_datetime_ultima_atualizacao,
                id_execucao_dbt as current_id_execucao_dbt
            from current_data

        {% else %}
            select
                cast(null as string) as journey_key,
                cast(null as bytes) as rider_id,
                cast(null as date) as service_date,
                cast(null as time) as boarding_time,
                cast(null as string) as boarding_servico,
                cast(null as bytes) as current_sha,
                datetime(null) as current_datetime_ultima_atualizacao,
                cast(null as string) as current_id_execucao_dbt
        {% endif %}
    ),
    sha_join as (
        select n.*, c.* except (rider_id, service_date, boarding_time, boarding_servico)
        from new_data_sha n
        left join
            current_data_sha c using (
                rider_id, service_date, boarding_time, boarding_servico
            )
    ),
    control_columns as (
        select
            ifnull(journey_key, generate_uuid()) as journey_key,
            * except (
                journey_key,
                new_sha,
                current_sha,
                current_datetime_ultima_atualizacao,
                current_id_execucao_dbt
            ),
            '{{ var("version") }}' as versao,
            case
                when current_sha is null or new_sha != current_sha
                then current_datetime("America/Sao_Paulo")
                else current_datetime_ultima_atualizacao
            end as datetime_ultima_atualizacao,
            case
                when current_sha is null or new_sha != current_sha
                then '{{ invocation_id }}'
                else current_id_execucao_dbt
            end as id_execucao_dbt
        from sha_join
    )
select *
from control_columns
