{% macro generate_date_hour_partition_filter(start_timestamp, end_timestamp) %}
    {% set start = modules.datetime.datetime.fromisoformat(start_timestamp) %}
    {% set end = modules.datetime.datetime.fromisoformat(end_timestamp) %}
    {% set days = (end.date() - start.date()).days %}

    {% if days == 0 %}
        data = date("{{ start_timestamp }}")
        and hora between {{ start.hour }} and {{ end.hour }}

    {% elif days == 1 %}
        (data = date("{{ start_timestamp }}") and hora >= {{ start.hour }})
        or (data = date("{{ end_timestamp }}") and hora <= {{ end.hour }})
    {% else %}
        (data = date("{{ start_timestamp }}") and hora >= {{ start.hour }})
        or (data > date("{{ start_timestamp }}") and data < date("{{ end_timestamp }}"))
        or (data = date("{{ end_timestamp }}") and hora <= {{ end.hour }})
    {% endif %}

{% endmacro %}
