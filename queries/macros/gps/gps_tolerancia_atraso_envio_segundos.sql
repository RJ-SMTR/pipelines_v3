{% macro gps_tolerancia_atraso_envio_segundos(datetime_envio, fonte) %}
  {% if fonte == "conecta" %}
        if(
            {{ datetime_envio }} >= datetime("2026-10-06 20:00:00")
            and {{ datetime_envio }} < datetime("2026-10-07 07:00:00"),
            12 * 3600,
            3600
        )
    {% else %} 3600
    {% endif %}
{% endmacro %}
