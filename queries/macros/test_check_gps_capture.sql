{% test check_gps_capture(model, table_id, interval) -%}
    {%- if execute -%}
        {%- if "sppo" in model -%}
            with
                t as (
                    select datetime(timestamp_array) as timestamp_array
                    from
                        unnest(
                            generate_timestamp_array(
                                timestamp("{{ var('date_range_start') }}"),
                                timestamp("{{ var('date_range_end') }}"),
                                interval {{ interval }} minute
                            )
                        ) as timestamp_array
                    where timestamp_array < timestamp("{{ var('date_range_end') }}")
                ),
                logs_table as (
                    select
                        safe_cast(
                            datetime(
                                timestamp(timestamp_captura), "America/Sao_Paulo"
                            ) as datetime
                        ) timestamp_captura,
                        safe_cast(sucesso as boolean) sucesso,
                        safe_cast(erro as string) erro,
                        safe_cast(data as date) data
                    from
                        `rj-smtr-staging.br_rj_riodejaneiro_onibus_gps_staging.{{ table_id }}_logs`
                        as t
                ),
                logs as (
                    select
                        *, timestamp_trunc(timestamp_captura, minute) as timestamp_array
                    from logs_table
                    where
                        data between date(
                            timestamp("{{ var('date_range_start') }}")
                        ) and date(timestamp("{{ var('date_range_end') }}"))
                        and timestamp_captura
                        between "{{ var('date_range_start') }}"
                        and "{{ var('date_range_end') }}"
                )
            select
                coalesce(
                    logs.timestamp_captura, t.timestamp_array
                ) as timestamp_captura,
                logs.erro
            from t
            left join logs on logs.timestamp_array = t.timestamp_array
            where logs.sucesso is not true
        {%- else -%}
            {%- if table_id == "registros" -%}
                -- depends_on: {{ ref('staging_gps') }}
                {% set table_ref = ref("staging_gps") %}
            {%- else -%}
                -- depends_on: {{ ref('staging_realocacao') }}
                {% set table_ref = ref("staging_realocacao") %}
            {%- endif -%}
            with
                t as (
                    select datetime(timestamp_array) as timestamp_array
                    from
                        unnest(
                            generate_timestamp_array(
                                timestamp("{{ var('date_range_start') }}"),
                                timestamp("{{ var('date_range_end') }}"),
                                interval {{ interval }} minute
                            )
                        ) as timestamp_array
                    where timestamp_array < timestamp("{{ var('date_range_end') }}")
                ),
                capture as (
                    select distinct datetime_captura
                    from {{ table_ref }}
                    where
                        (
                            {{
                                generate_date_hour_partition_filter(
                                    var("date_range_start"), var("date_range_end")
                                )
                            }}
                        )
                ),
                missing_timestamps as (
                    select t.timestamp_array as datetime_captura
                    from t
                    left join capture c on t.timestamp_array = c.datetime_captura
                    where c.datetime_captura is null
                )
            select *
            from missing_timestamps
        {%- endif -%}
    {%- endif -%}
{%- endtest %}
