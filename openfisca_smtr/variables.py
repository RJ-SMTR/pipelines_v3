# -*- coding: utf-8 -*-
"""Variaveis OpenFisca para remuneracao do subsidio SPPO."""

from openfisca_core.model_api import ETERNITY, MONTH, Enum, Variable, where

from openfisca_smtr.entities import apuracao
from openfisca_smtr.enums import ClassificacaoValidacao, TipoApuracao


class tipo_apuracao(Variable):
    value_type = Enum
    possible_values = TipoApuracao
    default_value = TipoApuracao.nao_apurada
    entity = apuracao
    definition_period = ETERNITY
    label = "Tipo de apuracao da viagem"


class classificacao_validacao(Variable):
    value_type = Enum
    possible_values = ClassificacaoValidacao
    default_value = ClassificacaoValidacao.invalida
    entity = apuracao
    definition_period = ETERNITY
    label = "Classificacao de validacao da viagem"


class km_programada(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = MONTH
    label = "Quilometragem programada da viagem"


class km_proporcional_incompleta(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = MONTH
    label = "Quilometragem proporcional reconhecida para viagem incompleta"


class indicador_quilometragem_pagamento(Variable):
    value_type = bool
    entity = apuracao
    definition_period = MONTH
    label = "Indica se a viagem conta para quilometragem de pagamento"

    def formula(apuracoes, period, parameters):
        tipo = apuracoes("tipo_apuracao", period)
        classificacao = apuracoes("classificacao_validacao", period)
        impacto = parameters(period).impacto_tipo_viagem

        return (
            (tipo == TipoApuracao.completa)
            * (
                (classificacao == ClassificacaoValidacao.conforme)
                * impacto.completa.conforme.quilometragem_pagamento
                + (classificacao == ClassificacaoValidacao.nao_conforme)
                * impacto.completa.nao_conforme.quilometragem_pagamento
                + (classificacao == ClassificacaoValidacao.invalida)
                * impacto.completa.invalida.quilometragem_pagamento
            )
            + (tipo == TipoApuracao.incompleta)
            * (
                (classificacao == ClassificacaoValidacao.conforme)
                * impacto.incompleta.conforme.quilometragem_pagamento
                + (classificacao == ClassificacaoValidacao.nao_conforme)
                * impacto.incompleta.nao_conforme.quilometragem_pagamento
                + (classificacao == ClassificacaoValidacao.invalida)
                * impacto.incompleta.invalida.quilometragem_pagamento
            )
            + (tipo == TipoApuracao.nao_apurada) * impacto.nao_apurada.quilometragem_pagamento
        )


class indicador_percentual_atendimento(Variable):
    value_type = bool
    entity = apuracao
    definition_period = MONTH
    label = "Indica se a viagem conta para percentual de atendimento"

    def formula(apuracoes, period, parameters):
        tipo = apuracoes("tipo_apuracao", period)
        classificacao = apuracoes("classificacao_validacao", period)
        impacto = parameters(period).impacto_tipo_viagem

        return (
            (tipo == TipoApuracao.completa)
            * (
                (classificacao == ClassificacaoValidacao.conforme)
                * impacto.completa.conforme.percentual_atendimento
                + (classificacao == ClassificacaoValidacao.nao_conforme)
                * impacto.completa.nao_conforme.percentual_atendimento
                + (classificacao == ClassificacaoValidacao.invalida)
                * impacto.completa.invalida.percentual_atendimento
            )
            + (tipo == TipoApuracao.incompleta)
            * (
                (classificacao == ClassificacaoValidacao.conforme)
                * impacto.incompleta.conforme.percentual_atendimento
                + (classificacao == ClassificacaoValidacao.nao_conforme)
                * impacto.incompleta.nao_conforme.percentual_atendimento
                + (classificacao == ClassificacaoValidacao.invalida)
                * impacto.incompleta.invalida.percentual_atendimento
            )
            + (tipo == TipoApuracao.nao_apurada) * impacto.nao_apurada.percentual_atendimento
        )


class km_remuneravel(Variable):
    value_type = float
    entity = apuracao
    definition_period = MONTH
    label = "Quilometragem remuneravel da viagem"

    def formula(apuracoes, period, parameters):
        tipo = apuracoes("tipo_apuracao", period)
        indicador_pagamento = apuracoes("indicador_quilometragem_pagamento", period)
        km_completa = apuracoes("km_programada", period)
        km_incompleta = apuracoes("km_proporcional_incompleta", period)
        km_base = where(tipo == TipoApuracao.incompleta, km_incompleta, km_completa)
        return where(indicador_pagamento, km_base, 0.0)


class viagens_programadas(Variable):
    value_type = int
    default_value = 0
    entity = apuracao
    definition_period = MONTH
    label = "Quantidade de viagens programadas no intervalo"


class viagens_atendimento(Variable):
    value_type = int
    default_value = 0
    entity = apuracao
    definition_period = MONTH
    label = "Quantidade de viagens que contam para atendimento"


class percentual_atendimento(Variable):
    value_type = float
    entity = apuracao
    definition_period = MONTH
    label = "Percentual de atendimento"

    def formula(apuracoes, period, parameters):
        atendimento = apuracoes("viagens_atendimento", period)
        programadas = apuracoes("viagens_programadas", period)
        return where(programadas > 0, atendimento / programadas, 0.0)


class ipa(Variable):
    value_type = float
    entity = apuracao
    definition_period = MONTH
    label = "Indicador de percentual de atendimento"

    def formula(apuracoes, period, parameters):
        percentual = apuracoes("percentual_atendimento", period)
        ipa_params = parameters(period).ipa
        return where(
            percentual >= ipa_params.thresholds[3],
            ipa_params.amounts[3],
            where(
                percentual >= ipa_params.thresholds[2],
                ipa_params.amounts[2],
                where(
                    percentual >= ipa_params.thresholds[1],
                    ipa_params.amounts[1],
                    ipa_params.amounts[0],
                ),
            ),
        )


class desconto_operacao_precaria(Variable):
    value_type = float
    entity = apuracao
    definition_period = MONTH
    label = "Desconto fixo por operacao precaria"

    def formula(apuracoes, period, parameters):
        percentual = apuracoes("percentual_atendimento", period)
        operacao_precaria = parameters(period).operacao_precaria
        return where(
            (percentual >= operacao_precaria.limite_grave)
            * (percentual < operacao_precaria.limite_moderado),
            operacao_precaria.desconto_moderado,
            where(
                percentual < operacao_precaria.limite_grave,
                operacao_precaria.desconto_grave,
                0.0,
            ),
        )


class idt(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = MONTH
    label = "Indice de desempenho de transporte aplicavel"


class prd(Variable):
    value_type = float
    entity = apuracao
    definition_period = MONTH
    label = "Percentual de reducao por desempenho"

    def formula(apuracoes, period, parameters):
        valor_idt = apuracoes("idt", period)
        prd_params = parameters(period).prd
        return where(
            valor_idt < prd_params.thresholds[1],
            prd_params.amounts[0],
            where(
                valor_idt < prd_params.thresholds[2],
                prd_params.amounts[1],
                where(
                    valor_idt < prd_params.thresholds[3],
                    prd_params.amounts[2],
                    where(
                        valor_idt < prd_params.thresholds[4],
                        prd_params.amounts[3],
                        prd_params.amounts[4],
                    ),
                ),
            ),
        )


class tarifa_remuneracao(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = MONTH
    label = "Tarifa de remuneracao do lote"


class alpha(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = MONTH
    label = "Percentual CAPEX da tarifa de remuneracao"


class beta(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = MONTH
    label = "Percentual OPEX da tarifa de remuneracao"


class km_referencia(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = MONTH
    label = "Quilometros operacionais de referencia da quinzena"


class fcf(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = MONTH
    label = "Fator cumprimento de frota"


class km_ponderada_ipa(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = MONTH
    label = "Soma de quilometragem conforme ponderada pelo IPA"


class receita_tarifa_publica(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = MONTH
    label = "Receita da tarifa publica na quinzena"


class saldo_compensacao_anterior(Variable):
    value_type = float
    default_value = 0.0
    entity = apuracao
    definition_period = MONTH
    label = "Saldo anterior de compensacao devido ao poder concedente"


class remuneracao_servico(Variable):
    value_type = float
    entity = apuracao
    definition_period = MONTH
    label = "Remuneracao pelo servico da concessionaria"

    def formula(apuracoes, period, parameters):
        tr = apuracoes("tarifa_remuneracao", period)
        alpha_lote = apuracoes("alpha", period)
        beta_lote = apuracoes("beta", period)
        km_ref = apuracoes("km_referencia", period)
        fcf_lote = apuracoes("fcf", period)
        km_ipa = apuracoes("km_ponderada_ipa", period)
        prd_lote = apuracoes("prd", period)
        return tr * (alpha_lote * km_ref * fcf_lote + beta_lote * km_ipa * (1 - prd_lote))


class subsidio_bruto(Variable):
    value_type = float
    entity = apuracao
    definition_period = MONTH
    label = "Subsidio bruto antes da compensacao intertemporal"

    def formula(apuracoes, period, parameters):
        remuneracao = apuracoes("remuneracao_servico", period)
        receita = apuracoes("receita_tarifa_publica", period)
        return remuneracao - receita


class subsidio_liquido(Variable):
    value_type = float
    entity = apuracao
    definition_period = MONTH
    label = "Subsidio liquido a pagar apos compensacao"

    def formula(apuracoes, period, parameters):
        bruto = apuracoes("subsidio_bruto", period)
        saldo_anterior = apuracoes("saldo_compensacao_anterior", period)
        return where(bruto > saldo_anterior, bruto - saldo_anterior, 0.0)


class saldo_compensacao_posterior(Variable):
    value_type = float
    entity = apuracao
    definition_period = MONTH
    label = "Saldo posterior de compensacao devido ao poder concedente"

    def formula(apuracoes, period, parameters):
        bruto = apuracoes("subsidio_bruto", period)
        saldo_anterior = apuracoes("saldo_compensacao_anterior", period)
        return where(
            bruto >= 0,
            where(saldo_anterior > bruto, saldo_anterior - bruto, 0.0),
            saldo_anterior - bruto,
        )


VARIABLES = [
    tipo_apuracao,
    classificacao_validacao,
    km_programada,
    km_proporcional_incompleta,
    indicador_quilometragem_pagamento,
    indicador_percentual_atendimento,
    km_remuneravel,
    viagens_programadas,
    viagens_atendimento,
    percentual_atendimento,
    ipa,
    desconto_operacao_precaria,
    idt,
    prd,
    tarifa_remuneracao,
    alpha,
    beta,
    km_referencia,
    fcf,
    km_ponderada_ipa,
    receita_tarifa_publica,
    saldo_compensacao_anterior,
    remuneracao_servico,
    subsidio_bruto,
    subsidio_liquido,
    saldo_compensacao_posterior,
]
