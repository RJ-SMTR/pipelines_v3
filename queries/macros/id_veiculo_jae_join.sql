{#
  Predicado de join viagem × Jaé (transacao / gps_validador).

  RIO: concat(lote do consórcio, id_veiculo Jaé) = id_veiculo da viagem.
  SPPO: id_veiculo Jaé = substr(id_veiculo da viagem, 2) [via id_veiculo_join_sppo].
#}
{% macro id_veiculo_jae_join(
    id_veiculo_jae, id_veiculo_viagem, lote, id_veiculo_join_sppo
) %}
    {%- if var("sistema") == "rio" -%}
        concat(
            coalesce(
                {{ lote }}, regexp_extract({{ id_veiculo_viagem }}, r'^([A-Z][0-9])')
            ),
            {{ id_veiculo_jae }}
        )
        = {{ id_veiculo_viagem }}
    {%- else -%} {{ id_veiculo_jae }} = {{ id_veiculo_join_sppo }}
    {%- endif -%}
{% endmacro %}
