{% macro add_to_datetime(datetime_str, hours=0, minutes=0, seconds=0, days=0) %}
    {% set dt = modules.datetime.datetime.fromisoformat(
        datetime_str
    ) + modules.datetime.timedelta(
        days=days, hours=hours, minutes=minutes, seconds=seconds
    ) %}
    {{ return(dt.strftime("%Y-%m-%dT%H:%M:%S")) }}
{% endmacro %}
