{% test check_gps_treatment(model, registros=none, aux_filtrada=none) -%}
    {% if model.identifier == "gps_sppo" %}
        -- depends_on: {{ ref('sppo_registros') }}
        -- depends_on: {{ ref('sppo_aux_registros_filtrada') }}
        -- depends_on: {{ ref('gps_sppo') }}
        {% set timestamp = "timestamp_gps" %}
        {% set ordem = "ordem" %}
        {% set linha = "linha" %}
        {% set registros = ref("sppo_registros") %}
        {% set aux_filtrada = ref("sppo_aux_registros_filtrada") %}
        {% set gps = ref("gps_sppo") %}
    {% else %}
        -- depends_on: {{ ref(registros) }}
        -- depends_on: {{ ref(aux_filtrada) }}
        {% set timestamp = "datetime_gps" %}
        {% set ordem = "id_veiculo" %}
        {% set linha = "servico" %}
        {% set registros = ref(registros) %}
        {% set aux_filtrada = ref(aux_filtrada) %}
        {% set gps = model %}
    {% endif %}

    {{
        check_gps_treatment_query(
            timestamp, ordem, linha, registros, aux_filtrada, gps
        )
    }}
{%- endtest %}

{% test check_gps_treatment_v2(model, registros, aux_filtrada) -%}
    -- depends_on: {{ ref(registros) }}
    -- depends_on: {{ ref(aux_filtrada) }}
    {% set registros = ref(registros) %}
    {% set aux_filtrada = ref(aux_filtrada) %}
    {% set gps = model %}

    {{
        check_gps_treatment_query(
            "datetime_gps", "id_registro", "servico", registros, aux_filtrada, gps
        )
    }}
{%- endtest %}
