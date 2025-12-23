-- fmt: off
{% macro generate_database_name(custom_database_name=none, node=none) -%}

    {%- set default_database = target.database -%}
    {% set dev_database = "rj-smtr-dev" %}
    {%- if custom_database_name is none -%}

        {% if target.name in ("dev", "hmg") %}

            {{ dev_database }}

        {% else %}

            {{ default_database }}

        {% endif %}

    {%- else -%}

        {{ custom_database_name | trim }}

    {%- endif -%}

{%- endmacro %}
