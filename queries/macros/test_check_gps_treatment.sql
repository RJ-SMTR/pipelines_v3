{% test check_gps_treatment(model) -%}
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
        -- depends_on: {{ ref('staging_gps', v=1) }}
        -- depends_on: {{ ref('aux_gps_filtrada', v=1) }}
        -- depends_on: {{ ref('gps', v=1) }}
        {% set timestamp = "datetime_gps" %}
        {% set ordem = "id_veiculo" %}
        {% set linha = "servico" %}
        {% set registros = ref("staging_gps", v=1) %}
        {% set aux_filtrada = ref("aux_gps_filtrada", v=1) %}
        {% set gps = ref("gps", v=1) %}
    {% endif %}

    {{
        check_gps_treatment_query(
            timestamp, ordem, linha, registros, aux_filtrada, gps
        )
    }}
{%- endtest %}

{% test check_gps_treatment_v2(model) -%}
    -- depends_on: {{ ref('staging_gps', v=2) }}
    -- depends_on: {{ ref('aux_gps_filtrada', v=2) }}
    -- depends_on: {{ ref('gps', v=2) }}
    {% set registros = ref("staging_gps", v=2) %}
    {% set aux_filtrada = ref("aux_gps_filtrada", v=2) %}
    {% set gps = ref("gps", v=2) %}

    {{
        check_gps_treatment_query(
            "datetime_gps", "id_registro", "servico", registros, aux_filtrada, gps
        )
    }}
{%- endtest %}
