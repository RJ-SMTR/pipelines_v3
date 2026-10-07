{{ config(materialized="view", schema="gtfs_staging", alias="feed_info") }}

select
    safe_cast(data_versao as date) as data_versao,
    safe_cast(feed_publisher_name as string) as feed_publisher_name,
    safe_cast(
        json_value(content, '$.feed_publisher_url') as string
    ) as feed_publisher_url,
    safe_cast(json_value(content, '$.feed_lang') as string) as feed_lang,
    safe_cast(json_value(content, '$.default_lang') as string) as default_lang,
    safe_cast(json_value(content, '$.feed_start_date') as string) as feed_start_date,
    safe_cast(json_value(content, '$.feed_end_date') as string) as feed_end_date,
    safe_cast(json_value(content, '$.feed_version') as string) as feed_version,
    safe_cast(
        json_value(content, '$.feed_contact_email') as string
    ) as feed_contact_email,
    safe_cast(json_value(content, '$.feed_contact_url') as string) as feed_contact_url,
    safe_cast(json_value(content, '$.feed_id') as string) as feed_id,
    timestamp_captura
from {{ source("br_rj_riodejaneiro_gtfs_staging", "feed_info") }}
