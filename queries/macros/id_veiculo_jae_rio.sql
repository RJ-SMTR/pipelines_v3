{% macro id_veiculo_jae_rio(id_operadora_expr, id_veiculo_expr) %}
    case
        when {{ id_operadora_expr }} = '2801'
        then 'A2-' || lpad(right({{ id_veiculo_expr }}, 3), 3, '0')
        when {{ id_operadora_expr }} = '2802'
        then 'B2-' || lpad(right({{ id_veiculo_expr }}, 3), 3, '0')
    end
{% endmacro %}
