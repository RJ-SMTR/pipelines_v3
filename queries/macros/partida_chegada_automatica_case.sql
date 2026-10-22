{% macro partida_chegada_automatica_case(
    agg, negate_dwithin, point, buffer, midpoint_op
) %}
    {{ agg }} (
        case
            when
                {% if negate_dwithin %}
                    not {% endif %} st_dwithin(
                    g.geo_point_gps,
                    sh.{{ point }},
                    {{ var("buffer_validacao_inicio_fim_metros") }}
                )
                and st_intersects(spu.{{ buffer }}, g.geo_point_gps)
                and g.datetime_gps {{ midpoint_op }} mp.datetime_midpoint
            then g.datetime_gps
        end
    )
{% endmacro %}
