/* https://discourse.getdbt.com/t/get-all-dbt-table-model-names-from-a-tag-inside-another-model/7703 (modificado) */
{% macro get_models_with_tags(tags) %}

{% set models_with_tag = [] %}

{% for model in graph.nodes.values() | selectattr("resource_type", "equalto", "model") %}

    {% for tag in tags %}
        {% if tag in model.config.tags %}
            {{ models_with_tag.append(model) }}
        {% endif %}
    {% endfor %}

{% endfor %}

{{ return(models_with_tag) }}

{% endmacro %}