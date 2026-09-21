{{
    config(
        materialized="incremental",
        partition_by={
            "field": "feed_start_date",
            "data_type": "date",
            "granularity": "day",
        },
        alias="feed_info",
        unique_key="feed_start_date",
        incremental_strategy="insert_overwrite",
    )
}}

with
    feed_info as (
        select
            safe_cast(timestamp_captura as string) as feed_version,
            safe_cast(data_versao as date) as feed_start_date,
            null as feed_end_date,
            feed_publisher_name,
            feed_publisher_url,
            feed_lang,
            default_lang,
            feed_contact_email,
            feed_contact_url,
            current_datetime("America/Sao_Paulo") as feed_update_datetime,
            '{{ var("version") }}' as versao_modelo
        from {{ ref("staging_feed_info") }}
        {% if is_incremental() %}
            where data_versao = date('{{ var("data_versao_gtfs") }}')
            union all
            select *
            from {{ this }}
            where feed_start_date != date('{{ var("data_versao_gtfs") }}')
        {% endif %}
    )
select
    feed_version,
    feed_start_date,
    date_sub(
        lead(date(feed_start_date)) over (order by feed_start_date), interval 1 day
    ) as feed_end_date,
    feed_publisher_name,
    feed_publisher_url,
    feed_lang,
    default_lang,
    feed_contact_email,
    feed_contact_url,
    feed_update_datetime,
    versao_modelo
from feed_info
