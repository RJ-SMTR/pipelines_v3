{#
  Mapeia o nome do consórcio (Jaé ou agency_name GTFS) para o lote RIO
  (`A2`/`B2`/…). Fonte: var `lotes_consorcio_rio` em dbt_project.yml.
#}
{% macro lote_consorcio_rio(consorcio_expr) %}
    case
        {%- for nome, lote in var("lotes_consorcio_rio", {}).items() %}
            when upper({{ consorcio_expr }}) like '%{{ nome }}%' then '{{ lote }}'
        {%- endfor %}
    end
{% endmacro %}
