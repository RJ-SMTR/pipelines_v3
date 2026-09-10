{% macro is_current_state_enabled() %}
    {#
        controla o config enabled dos modelos de estado atual (foto da última
        captura da janela date_range_start/end):
        - true quando date_range_end alcança a data corrente (run diário)
        - true quando date_range_end é o default do projeto (parse, docs e
          execuções sem vars), para o modelo aparecer na linhagem do dbt docs
        - false em backfills com janela explícita antiga, evitando regredir
          a foto de estado atual
    #}
    {% set default_end = "2022-01-02T01:00:00" %}
    {% set end_value = var("date_range_end") %}
    {% if end_value == default_end %} {{ return(true) }} {% endif %}
    {% set end_date = modules.datetime.datetime.fromisoformat(end_value).date() %}
    {% set current_date = modules.datetime.datetime.now(
        modules.pytz.timezone("America/Sao_Paulo")
    ).date() %}
    {{ return(end_date >= current_date) }}
{% endmacro %}
