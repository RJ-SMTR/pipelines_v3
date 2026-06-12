{% macro ordem_servico_excecoes_join(o, t, include_excep=false) %}
    (
        (
            {{ o }}.tipo_dia = {{ t }}.tipo_dia
            and {{ o }}.tipo_os not in ("ENEM", "V+ENEM")
        )
        or (
            {{ o }}.feed_start_date = "2026-01-26"
            and {{ o }}.tipo_dia = "Ponto Facultativo"
            and {{ t }}.tipo_dia = "Sabado"
            and {{ o }}.servico in ("LECD126")
        )
        or ({{ o }}.tipo_dia = "Ponto Facultativo" and {{ t }}.tipo_dia = "Dia Útil")
        or (
            {{ o }}.feed_start_date = "2025-11-08"
            and {{ o }}.tipo_os in ("ENEM", "V+ENEM")
            and {{ o }}.tipo_dia = "Domingo"
            and {{ t }}.tipo_dia = "Dia Útil"
            and {{ o }}.servico not in ("LECD126", "SE867")
        )
        or (
            {{ o }}.feed_start_date = "2025-11-08"
            and {{ o }}.tipo_os in ("ENEM", "V+ENEM")
            and {{ o }}.tipo_dia = {{ t }}.tipo_dia
            and {{ o }}.servico in ("LECD126", "SE867")
        )
        or (
            {{ o }}.feed_start_date = "2025-12-21"
            and {{ o }}.tipo_os = "Verão"
            and {{ t }}.tipo_dia = "Dia Útil"
            and (
                ({{ o }}.servico = '616' and {{ o }}.tipo_dia in ('Sabado', 'Domingo'))
                or ({{ o }}.servico = '913' and {{ o }}.tipo_dia = 'Domingo')
                or ({{ o }}.servico = '485' and {{ o }}.tipo_dia = 'Sabado')
            )
        )
        or (
            {{ o }}.feed_start_date = "2025-12-27"
            and {{ o }}.tipo_os = "Reveillon_01-01"
            and {{ t }}.tipo_dia = "Sabado"
            and (
                {{ o }}.servico in (
                    "SE457",
                    "SE474",
                    "SE553",
                    "SE397",
                    "SE388",
                    "SE100",
                    "SE298",
                    "SE355",
                    "SE363",
                    "SE265",
                    "SE169",
                    "SE483",
                    "SE324",
                    "SE383",
                    "SE393",
                    "SE554",
                    "SE238",
                    "SE399",
                    "SE550",
                    "SEB232",
                    "SE312",
                    "SE328",
                    "SE368"
                )
                and {{ o }}.tipo_dia = 'Domingo'
            )
        )
        {% if include_excep %} or ({{ t }}.tipo_dia = "EXCEP") {% endif %}
    )
{% endmacro %}
