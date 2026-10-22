{% test test_shape_id_gtfs(model, column_name) %}
    select distinct t.trip_short_name
    from {{ model }} t
    left join {{ ref("shapes_gtfs") }} s using (feed_start_date, {{ column_name }})
    where s.shape_id is null and t.feed_start_date = '{{ var("data_versao_gtfs") }}'
{% endtest %}
