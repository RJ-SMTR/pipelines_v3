{#
  WIP/teste sistemario: lote A0 quando o serviço não está no POR.

  Para desligar: apagar `lote_padrao` em dbt_project.yml (o macro vira no-op).
  Para remover: apagar este arquivo, a var e as chamadas `lote_padrao_teste(...)`.
#}
{% macro lote_padrao_teste(expr) %}
    {%- set lote_padrao = var("lote_padrao", none) -%}
    {%- if lote_padrao -%}
        coalesce(nullif({{ expr }}, ""), '{{ lote_padrao }}')
    {%- else -%} {{ expr }}
    {%- endif -%}
{% endmacro %}
