{% macro gps_tolerancia_atraso_envio_segundos(datetime_envio) %}
    {#
        Excepcionalidade GPS de 2026-10-06 20h até 2026-10-07 07h (exclusive).
        Ofício Consórcios nº 184/2026.
        Nesse intervalo o atraso entre envio e servidor pode chegar a 12 horas.
        Fora dele, a tolerância permanece de 1 hora.
    #}
    if(
        {{ datetime_envio }} >= datetime("2026-10-06 20:00:00")
        and {{ datetime_envio }} < datetime("2026-10-07 07:00:00"),
        12 * 3600,
        3600
    )
{% endmacro %}
