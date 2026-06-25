{% macro get_last_feed_start_date(data_versao_gtfs) %}
    {% set result = run_query(
        "SELECT MAX(feed_start_date) FROM "
        ~ ref("feed_info_gtfs")
        ~ " WHERE feed_start_date < "
        ~ "'"
        ~ data_versao_gtfs
        ~ "'"
    ) %}
    {% set last_feed_start_date = result.columns[0].values()[0] %}
    {{ return(last_feed_start_date or data_versao_gtfs) }}
{% endmacro %}
