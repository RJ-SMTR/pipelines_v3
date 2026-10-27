# MIT / IPEA project: stop-based origin-destination inference

## What the model does

Jaé records a tap when a rider boards, but never when they alight. The model
recovers the alighting stop with the standard trip-chaining assumption: a rider's
next boarding is close to where they got off the previous bus.

1. **Boarding stop.** Match the tap's vehicle to the GPS-derived stop visit closest
   in time to the tap, on the route the ticketing registry assigns to the service.
2. **Next origin.** The rider's next tap of the day gives the next boarding
   location. For the last tap of the day the first tap of the day is used instead.
3. **Alighting stop.** Among later stops on the same route direction that the
   vehicle reached within two hours, pick the one closest to the next origin.
4. **Confidence tier.** Label each prediction high, medium or low from the walking
   distance, the margin over the runner-up stop and whether the next origin was an
   observed later tap.
5. **Survey.** Package the prediction with three neighbouring stops as four
   shuffled options, one row per rider journey.

## Model chain (`v1/`)

Build order, with what each model reads:

1. `vw_stops` ← `rj-smtr.gtfs.stops`
2. `vw_h3` ← `rj-smtr.br_rj_riodejaneiro_geo.h3_res9`
3. `route_stops` ← `vw_stops`, `rj-smtr.gtfs.trips`, `stop_times`, `routes`
4. `ticketing` ← sample table or Jaé `transacao`, `vw_h3`,
   `rj-smtr.cadastro.servicos`, `rj-smtr.gtfs.routes`
5. `stop_events` ← `route_stops`, `rj-smtr.monitoramento.view_gps_onibus`,
   `rj-smtr.gtfs.stops` (to pick the active feed)
6. `origin_destination_stop` ← `ticketing`, `stop_events`
7. `origin_destination_stop_candidates` ← `origin_destination_stop`, `route_stops`
8. `survey_journeys` ← `ticketing`, `origin_destination_stop`,
   `origin_destination_stop_candidates`, `vw_stops`

`vw_*` models are views and `route_stops` is a table. Every other model is
incremental with one partition per `service_date`, overwritten on each run.

| Model                                | Grain                     | Purpose                                                            |
|--------------------------------------|---------------------------|--------------------------------------------------------------------|
| `vw_stops`                           | stop × feed               | GTFS boarding points as GEOGRAPHY                                  |
| `vw_h3`                              | H3 res-9 tile             | Tile geometry and centroid, sample source only                     |
| `route_stops`                        | route × dir × stop × feed | Ordered stops per route direction, feeds from 2025                 |
| `ticketing`                          | tap                       | Cleaned Jaé taps with route, vehicle, chain position               |
| `stop_events`                        | vehicle stop visit        | GPS pings snapped to stops, with `is_forward` direction flag       |
| `origin_destination_stop`            | tap                       | Boarding and alighting stops, walk distances, `confidence_tier`    |
| `origin_destination_stop_candidates` | tap × option              | Prediction plus 3 neighbours in shuffled display order             |
| `survey_journeys`                    | journey                   | Integration-linked journeys with the final leg's 4 options pivoted |

### Tunable constants

All thresholds are Jinja `set` constants at the top of each model.

| Model                                | Constant                        | Value    | Meaning                                       |
|--------------------------------------|---------------------------------|----------|-----------------------------------------------|
| `stop_events`                        | `max_snap_m`                    | 300      | Max ping-to-stop distance for a visit         |
| `origin_destination_stop`            | `max_boarding_gap_s`            | 600      | Tap time vs stop visit tolerance              |
| `origin_destination_stop`            | `max_walk_m`                    | 1000     | Walk cap for `alighting_within_walk_cap`      |
| `origin_destination_stop`            | `walk_high_m`, `walk_low_m`     | 300, 700 | Walk bounds for high and low tiers            |
| `origin_destination_stop`            | `margin_high_m`, `margin_low_m` | 150, 50  | Margin over runner-up (stops ~290 m apart)    |
| `origin_destination_stop_candidates` | `n_alternates`, `max_offset`    | 3, 3     | Alternates shown and search span along route  |
| `survey_journeys`                    | `max_transfer_gap_minutes`      | 120      | Gap that breaks a journey despite integration |

## Running the models

All daily models are driven by the `run_date` variable and overwrite that day's
partition. Build from `queries/` with `--profiles-dir ./dev` on the `sandbox` target, which writes to
`rj-smtr-sandbox` with your own Google identity (`gcloud auth application-default login` once).

```bash
cd queries
dbt deps

# Full chain for one day, against the anonymised sample
dbt build --select mit_ipea_project.v1 --target sandbox --profiles-dir ./dev \
  --vars '{"run_date": "2026-09-28"}'

# Against the production Jaé table (needs Fine-Grained Reader on hash_cliente)
dbt build --select mit_ipea_project.v1 --target sandbox --profiles-dir ./dev \
  --vars '{"run_date": "2026-09-28", "ticketing_source": "jae"}'

# Rebuild the static GTFS models only
dbt run --select vw_stops route_stops --target sandbox --profiles-dir ./dev

# Check SQL without running it
dbt compile --select origin_destination_stop --target sandbox --profiles-dir ./dev \
  --vars '{"run_date": "2026-09-28"}'
```

Always run `sqlfmt` on any model you edit.

### Ticketing source

`ticketing` reads one of two sources, chosen by the `ticketing_source` variable:

| Value              | Table                                                   | Tap location          | Notes                     |
|--------------------|---------------------------------------------------------|-----------------------|---------------------------|
| `sample` (default) | `rj-smtr-sandbox.mit_ipea_project.transacao_drc_sample` | H3 tile centroid      | Anonymised, rounded times |
| `jae`              | `rj-smtr.br_rj_riodejaneiro_bilhetagem.transacao`       | Validator coordinates | Policy-tagged, never run  |

Any other value raises a compile error.

## Survey design

The survey shows each rider four stop photos for the final leg of a journey and
asks which stop they got off at. The models output stop ids, names and
coordinates; Street View images are built downstream from the coordinates. The
four options are the predicted stop, the stop before it, the stop after it, and
the closer of the two stops two places away. Options are shuffled by
`display_order`; `prediction_slot` says where the prediction landed and is for
analysis only. Journeys with fewer than four options are excluded. The
questionnaire should offer a "none of these" answer.

`survey_journeys` keeps every confidence tier, including predictions beyond the walk
cap, so a batch can be stratified by `confidence_tier` when drawn.

## Archive (`archive/`)

Earlier tile-based (H3) models and exploratory views kept for reference. They are
not part of the survey chain, but most are still enabled and will build if you
select the whole folder, so select `mit_ipea_project.v1` rather than
`mit_ipea_project`.

| Model                                            | Notes                                                               |
|--------------------------------------------------|---------------------------------------------------------------------|
| `h3_gps`, `h3_stops`                             | GPS pings and stops assigned to H3 tiles                            |
| `origin_destination`, `origin_destination_stops` | Tile-based OD baseline and its stop variant                         |
| `origin_destination_land_use_power1`             | Tile-based OD with land-use gravity weighting                       |
| `vw_utilization`                                 | Tile-based vehicle utilisation                                      |
| `vw_utilization_land_use_gravity`                | Disabled, incomplete SQL                                            |
| `vw_vehicle_details`                             | Vehicle per licensing day from `cadastro.veiculo_licenciamento_dia` |

## Known limitations and next steps

- Sample tap times are rounded, so `boarding_gap_s` is noisy on the sample source.
- Single-tap days and taps with no next boarding location get no alighting
  prediction.
- Taps on BRT, VLT, metro, vans and unmatched buses still serve as a next origin
  via their `tap_location`, but never receive a prediction themselves.
- The `jae` source branch needs its first production run and a check that
  `vehicle_id` parsing holds on real `id_veiculo` values.
- The short-name fallback in `ticketing` takes the route from the latest GTFS feed
  overall, not the feed in force on `run_date`.
- Confidence tier thresholds are heuristics awaiting calibration against survey
  answers.
- Porting `vw_utilization` from tiles to stop segments is the next model to build.

## Key design decisions

- **GPS carries no route or direction.** The GPS view has null `route_id`,
  `direction_id` and `trip_id` for every provider. Only `servico` (the GTFS
  `route_short_name`) is usable, so `stop_events` matches each ping against the
  stops of every route and direction carrying that service, and the consumer picks
  the direction.
- **Jaé `sentido` is unreliable.** It matches GTFS `direction_id` (0 = ida,
  1 = volta) but was wrong for roughly 18% of taps on the 2026-09-28 sample.
  `origin_destination_stop` tries the stated direction first and falls back to the
  opposite one when the vehicle's GPS trend disagrees. `direction_source` records
  which applied. On that sample, fallback taps validated as well as the rest.
- **Service registry gaps.** `cadastro.servicos` has no GTFS link for some bus
  services (new services and SV/R variants, e.g. 917, SV917, 804R). `ticketing`
  falls back to the GTFS route whose short name equals the Jaé service name.
  `route_source` records which path was used.
- **Direction stops sit across the road.** Every ping produces a mirror visit in the
  opposite direction whose stop sequence runs backwards. `is_forward` filters these
  out so the boarding stop is never the one across the street.
- **Spatial join first, equality join second.** In `stop_events` the `st_dwithin`
  join runs against one row per stop so BigQuery can use its spatial join
  optimisation. Joining `route_stops` directly in the spatial predicate made the
  first version run for hours.
- **BRT cannot be matched.** Jaé sets `id_veiculo` to null for BRT, and the BRT GPS
  view has no usable service. Report funnels for `mode = 'Ônibus'` only.
- **Vehicle matching.** `ticketing.vehicle_id` is the 5-digit vehicle number;
  GPS-based models join on `right(gps.vehicle_id, 5)`.
