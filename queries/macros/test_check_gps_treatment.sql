{% test check_gps_treatment(model) -%}

    {% if "gps_sppo" in model %}
        -- depends_on: {{ ref('sppo_registros') }}
        -- depends_on: {{ ref('sppo_aux_registros_filtrada') }}
        -- depends_on: {{ ref('gps_sppo') }}
        {% set timestamp = "timestamp_gps" %}
        {% set ordem = "ordem" %}
        {% set linha = "linha" %}
        {% set registros = ref("sppo_registros") %}
        {% set aux_filtrada = ref("sppo_aux_registros_filtrada") %}
        {% set gps = ref("gps_sppo") %}
    {% else %}
        -- depends_on: {{ ref('staging_gps') }}
        -- depends_on: {{ ref('aux_gps_filtrada') }}
        -- depends_on: {{ ref('gps') }}
        {% set timestamp = "datetime_gps" %}
        {% set ordem = "id_veiculo" %}
        {% set linha = "servico" %}
        {% set registros = ref("staging_gps") %}
        {% set aux_filtrada = ref("aux_gps_filtrada") %}
        {% set gps = ref("gps") %}
    {% endif %}

    with
        data_hora as (
            select
                extract(date from timestamp_array) as data,
                extract(hour from timestamp_array) as hora,
            from
                unnest(
                    generate_timestamp_array(
                        "{{ var('date_range_start') }}",
                        "{{ var('date_range_end') }}",
                        interval 1 hour
                    )
                ) as timestamp_array
        ),
        gps_data as (
            select data, {{ timestamp }}, latitude, longitude
            from {{ registros }}
            where
                (
                    {{
                        generate_date_hour_partition_filter(
                            var("date_range_start"),
                            add_to_datetime(var("date_range_end"), seconds=1),
                        )
                    }}
                )
                and {{ timestamp }}
                between datetime("{{ var('date_range_start') }}") and datetime(
                    "{{ var('date_range_end') }}"
                )
            qualify
                row_number() over (
                    partition by {{ ordem }}, {{ timestamp }}, {{ linha }}
                )
                = 1
        ),
        gps_raw as (
            select
                extract(date from {{ timestamp }}) as data,
                extract(hour from {{ timestamp }}) as hora,
                count(*) as q_gps_raw
            from gps_data
            group by 1, 2
        ),
        gps_filtrada as (
            select
                extract(date from {{ timestamp }}) as data,
                extract(hour from {{ timestamp }}) as hora,
                count(*) as q_gps_filtrada
            from
                -- `rj-smtr.br_rj_riodejaneiro_onibus_gps.sppo_aux_registros_filtrada`
                {{ aux_filtrada }}
            where
                {% if "gps_sppo" in model %}
                    data between date("{{ var('date_range_start') }}") and date(
                        "{{ add_to_datetime(var('date_range_end'), seconds=1) }}"
                    )
                    and
                {% endif %}
                {{ timestamp }}
                between datetime("{{ var('date_range_start') }}") and datetime(
                    "{{ var('date_range_end') }}"
                )
            group by 1, 2
        ),
        gps_sppo as (
            select
                data,
                extract(hour from {{ timestamp }}) as hora,
                count(*) as q_gps_treated
            from
                -- `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
                {{ gps }}
            where
                data between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
            group by 1, 2
        ),
        gps_join as (
            select
                *,
                safe_divide(q_gps_filtrada, q_gps_raw) as indice_tratamento_raw,
                safe_divide(
                    q_gps_treated, q_gps_filtrada
                ) as indice_tratamento_filtrada,
                case
                    when
                        q_gps_raw = 0
                        or q_gps_filtrada = 0
                        or q_gps_treated = 0  -- Hipótese de perda de dados no tratamento
                        or q_gps_raw is null
                        or q_gps_filtrada is null
                        or q_gps_treated is null  -- Hipótese de perda de dados no tratamento
                        or (q_gps_raw < q_gps_filtrada)
                        or (q_gps_filtrada < q_gps_treated)  -- Hipótese de duplicação de dados
                        or (coalesce(safe_divide(q_gps_filtrada, q_gps_raw), 0) < 0.96)  -- Hipótese de perda de dados no tratamento (superior a 3%)
                        or (
                            coalesce(safe_divide(q_gps_treated, q_gps_filtrada), 0)
                            < 0.96
                        )  -- Hipótese de perda de dados no tratamento (superior a 3%)
                    then false
                    else true
                end as status
            from data_hora
            left join gps_raw using (data, hora)
            left join gps_filtrada using (data, hora)
            left join gps_sppo using (data, hora)
        )
    select *
    from gps_join
    where status is false
{%- endtest %}
