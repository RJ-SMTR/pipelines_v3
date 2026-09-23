{{
    config(
        materialized="view",
        schema="gtfs_staging",
    )
}}

select
    timestamp_captura as feed_version,
    data_versao as feed_start_date,
    feed_publisher_name,
    feed_publisher_url,
    feed_lang,
    default_lang,
    feed_contact_email,
    feed_contact_url
from {{ ref("base_feed_info") }}
