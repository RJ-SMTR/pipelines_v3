{{
    config(
        partition_by={
            "field": "data",
            "data_type": "date",
            "granularity": "day",
        },
        materialized="incremental",
        incremental_strategy="insert_overwrite",
    )
}}

{% set feed_info_gtfs = ref("feed_info_gtfs") %}
{% set calendar_gtfs = ref("calendar_gtfs") %}
{% set calendar_dates_gtfs = ref("calendar_dates_gtfs") %}
{% set calendario_manual = ref("aux_calendario_manual") %}

{# {% set feed_info_gtfs = "rj-smtr.gtfs.feed_info" %} #}
{# {% set calendar_gtfs = "rj-smtr.gtfs.calendar" %} #}
{# {% set calendar_dates_gtfs = "rj-smtr.gtfs.calendar_dates" %} #}
{# {% set calendario_manual = "rj-smtr.planejamento_staging.aux_calendario_manual" %} #}

{% if execute %}
    {% if is_incremental() %}
        {% set gtfs_feeds_query %}
            select concat("'", feed_start_date, "'") as feed_start_date
            from {{ feed_info_gtfs }}
            where
                feed_start_date <= date("{{ var('date_range_end') }}")
                and (feed_end_date IS NULL OR feed_end_date >= date("{{ var('date_range_start') }}"))

            union distinct

            select distinct concat("'", feed_start_date, "'") as feed_start_date
            from {{ calendario_manual }}
            where
                data between date("{{ var('date_range_start') }}")
                and date("{{ var('date_range_end') }}")
                and feed_start_date is not null
        {% endset %}

        {% set gtfs_feeds = run_query(gtfs_feeds_query).columns[0].values() %}
    {% endif %}
{% endif %}

with
    calendar as (
        select *
        from {{ calendar_gtfs }}
        {% if is_incremental() %}
            where feed_start_date in ({{ gtfs_feeds | join(", ") }})
        {% endif %}
    ),
    datas as (
        select data, extract(dayofweek from data) as dia_semana, feed_start_date
        from
            {{ feed_info_gtfs }},
            unnest(
                generate_date_array(
                    {% if is_incremental() %}
                        date("{{ var('date_range_start') }}"),
                        date("{{ var('date_range_end') }}")
                    {% else %}date("2024-10-01"), current_date("America/Sao_Paulo")
                    {% endif %}
                )
            ) as data
        where
            data >= feed_start_date and (data <= feed_end_date or feed_end_date is null)
    ),
    modificacao_manual as (
        select
            d.data,
            d.dia_semana,
            coalesce(m.feed_start_date, d.feed_start_date) as feed_start_date,
            m.tipo_dia,
            ifnull(m.tipo_os, "Regular") as tipo_os
        from datas d
        left join {{ calendario_manual }} m using (data)
    ),
    calendar_dates as (
        select
            cd.date as data,
            m.tipo_dia,
            m.tipo_os,
            case
                when cd.exception_type = '2'
                then
                    regexp_replace(
                        cd.service_id,
                        "^[U|S|D]_",
                        case
                            when extract(dayofweek from cd.date) = 7
                            then "S_"
                            when extract(dayofweek from cd.date) = 1
                            then "D_"
                            else "U_"
                        end
                    )
                else cd.service_id
            end as service_id,
            cd.exception_type,
            cd.feed_start_date,
        from {{ calendar_dates_gtfs }} cd
        join
            modificacao_manual m
            on cd.date = m.data
            and cd.feed_start_date = m.feed_start_date
        where
            {% if is_incremental() %}
                cd.feed_start_date in ({{ gtfs_feeds | join(", ") }})
                and cd.date between date("{{ var('date_range_start') }}") and date(
                    "{{ var('date_range_end') }}"
                )
            {% else %} date <= current_date("America/Sao_Paulo")
            {% endif %}
    ),
    datas_service_id as (
        select d.data, d.tipo_dia, d.tipo_os, c.service_id, d.feed_start_date
        from modificacao_manual d
        join
            calendar c
            on d.feed_start_date = c.feed_start_date
            and (
                (d.dia_semana = 1 and c.sunday = '1')
                or (d.dia_semana = 2 and c.monday = '1')
                or (d.dia_semana = 3 and c.tuesday = '1')
                or (d.dia_semana = 4 and c.wednesday = '1')
                or (d.dia_semana = 5 and c.thursday = '1')
                or (d.dia_semana = 6 and c.friday = '1')
                or (d.dia_semana = 7 and c.saturday = '1')
            )
    ),
    service_ids_retirados as (
        select d.*
        from datas_service_id d
        left join
            (select * from calendar_dates where exception_type = '2') c using (
                data, service_id
            )
        where c.service_id is null

    ),
    service_ids_adicionados as (
        select data, tipo_dia, tipo_os, service_id, feed_start_date
        from service_ids_retirados

        union distinct

        select data, tipo_dia, tipo_os, service_id, feed_start_date
        from calendar_dates
        where exception_type = '1'
    ),
    service_id_corrigido as (
        select
            data,
            tipo_dia,
            tipo_os,
            case
                when tipo_dia = "Domingo"
                then regexp_replace(service_id, "^[U|S]_", "D_")
                when tipo_dia = "Sabado"
                then regexp_replace(service_id, "^[U|D]_", "S_")
                when tipo_dia = "Dia Útil"
                then regexp_replace(service_id, "^[S|D]_", "U_")
                else service_id
            end as service_id,
            feed_start_date
        from service_ids_adicionados
    ),
    service_id_agg as (
        select
            data,
            tipo_dia,
            tipo_os,
            feed_start_date,
            array_agg(service_id) as service_ids
        from service_id_corrigido
        group by 1, 2, 3, 4
    )
select
    data,
    case
        when c.tipo_dia is not null
        then c.tipo_dia
        when "D_REG" in unnest(c.service_ids)
        then "Domingo"
        when "S_REG" in unnest(c.service_ids)
        then "Sabado"
        when "U_REG" in unnest(c.service_ids)
        then "Dia Útil"
    end as tipo_dia,
    case
        when c.tipo_os = "Extraordinária - Verão"
        then "Verão"
        when c.tipo_os like "%Madonna%"
        then "Madonna"
        when
            data between date(2025, 05, 03) and date(2025, 05, 04)
            and c.tipo_os = "Dia Atípico"
        then "Lady Gaga"  -- Processo.Rio MTR-PRO-2025/04520 [Operação Especial "Todo Mundo no Rio" - Lady Gaga]
        when
            data = date(2025, 05, 24)
            and c.tipo_os = "Dia Atípico"
        then "Marcha para Jesus"  -- [processo] [Operação especial evento "Marcha para Jesus"]
        when c.tipo_os = "Regular"
        then null
        else c.tipo_os
    end as subtipo_dia,
    c.tipo_os,
    c.service_ids,
    i.feed_version,
    c.feed_start_date,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
from service_id_agg c
join {{ feed_info_gtfs }} i using (feed_start_date)
