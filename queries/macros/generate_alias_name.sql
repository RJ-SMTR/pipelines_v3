{% macro generate_alias_name(custom_alias_name=none, node=none) -%}
    {%- set alias = dbt.generate_alias_name(custom_alias_name, node) | trim -%}
    {%- if replay_subsidy() -%} {{- (node.config.schema or "dbt") ~ "__" ~ alias -}}
    {%- else -%} {{- alias -}}
    {%- endif -%}
{%- endmacro %}
