{#
  Predicado de join viagem × Jaé (transacao / gps_validador).

  RIO: id_veiculo da viagem é `{lote}-{prefixo}` (ex. `A2-001`).
  Lote = left(id_veiculo, 2) (dois caracteres antes do hífen).
  concat(lote, '-', id_veiculo Jaé) = id_veiculo da viagem.
  SPPO: id_veiculo Jaé = substr(id_veiculo da viagem, 2) [via id_veiculo_join_sppo].
#}
{% macro id_veiculo_jae_join(
    id_veiculo_jae, id_veiculo_viagem, lote, id_veiculo_join_sppo
) %}
    {%- if var("sistema") == "rio" -%}
        concat(left({{ id_veiculo_viagem }}, 2), "-", {{ id_veiculo_jae }})
        = {{ id_veiculo_viagem }}
    {%- else -%} {{ id_veiculo_jae }} = {{ id_veiculo_join_sppo }}
    {%- endif -%}
{% endmacro %}
