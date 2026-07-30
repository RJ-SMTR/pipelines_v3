{% macro normalize_text(
    text, snake_case=false, case=none, remove_multiple_spaces=true
) %}
    {%- set expr = (
        "regexp_replace(normalize(" ~ text ~ ", nfd), r'[^A-Za-z0-9 ]', '')"
    ) -%}

    {%- if remove_multiple_spaces -%}
        {%- set expr = "trim(regexp_replace(" ~ expr ~ ", r'\\s+', ' '))" -%}
    {%- endif -%}

    {%- if snake_case -%}
        {%- set expr = "replace(" ~ expr ~ ", ' ', '_')" -%}
    {%- endif -%}

    {%- if case == "lower" -%} {%- set expr = "lower(" ~ expr ~ ")" -%}
    {%- elif case == "upper" -%} {%- set expr = "upper(" ~ expr ~ ")" -%}
    {%- endif -%}

    {{ return(expr) }}
{% endmacro %}
