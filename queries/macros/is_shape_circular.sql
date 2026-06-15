{% macro is_shape_circular(start_pt, end_pt, feed_start_date, shape_id) %}
    (
        (
            round(st_y({{ start_pt }}), 4) = round(st_y({{ end_pt }}), 4)
            and round(st_x({{ start_pt }}), 4) = round(st_x({{ end_pt }}), 4)
        )
        or (
            {{ feed_start_date }} >= date("{{ var('DATA_GTFS_V4_INICIO') }}")
            and trunc(st_y({{ start_pt }}), 4) = trunc(st_y({{ end_pt }}), 4)
            and trunc(st_x({{ start_pt }}), 4) = trunc(st_x({{ end_pt }}), 4)
        )
        or (
            {{ feed_start_date }} in (date("2025-05-01"), date("2025-12-27"))
            and {{ shape_id }} in ("iz18", "ycug")
        )  -- Operação Especial "Todo Mundo no Rio" - Lady Gaga e Reveillon 2025
    )
{% endmacro %}
