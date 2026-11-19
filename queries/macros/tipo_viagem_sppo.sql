{#
  Reclassificação do tipo_viagem do SPPO: tipo base, temperatura e bilhetagem.
  Sem os argumentos de bilhetagem, aplica só a regra de temperatura.
#}
{% macro tipo_viagem_sppo(
    data,
    tipo_viagem,
    detectado_ar_inoperante,
    sem_transacao=none,
    sem_transacao_tipo=none,
    estado_equipamento_aberto=none,
    validador_fechado=none,
    validador_associado_incorretamente=none
) %}
    case
        when
            {{ tipo_viagem }} not in (
                "Licenciado com ar e não autuado", "Licenciado sem ar e não autuado"
            )
        then {{ tipo_viagem }}
        when {{ detectado_ar_inoperante }}
        then "Detectado com ar inoperante"
        {%- if sem_transacao_tipo is not none %}
            when {{ sem_transacao_tipo }}
            then
                case
                    when
                        {{ data }} < date('{{ var("DATA_SUBSIDIO_V99_INICIO") }}')
                        and not {{ sem_transacao }}
                        and {{ estado_equipamento_aberto }}
                    then {{ tipo_viagem }}
                    else "Sem transação"
                end
            when {{ validador_fechado }}
            then "Validador fechado"
            when {{ validador_associado_incorretamente }}
            then "Validador associado incorretamente"
        {%- endif %}
        else {{ tipo_viagem }}
    end
{% endmacro %}
