-- fmt: off
{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}

        {% set schema_name = default_schema %}

    {%- else -%}

        {% set schema_name = custom_schema_name | trim %}

    {%- endif -%}

    {% if target.name == "dev" %}
        {% set schema_name = env_var("DBT_USER") + "__" + schema_name %}
    {% endif %}

    {% if target.name == "hmg" and schema_name.endswith("_staging") %}
        {% set schema_name = schema_name + "_dbt" %}
    {% endif %}

    {{ schema_name }}

{%- endmacro %}
