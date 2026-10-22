/*
  Etapa de tratamento com base na resposta aos ofícios MTR-OFI-2024/03024, MTR-OFI-2024/03025, MTR-OFI-2024/03026 e MTR-OFI-2024/03027 (Processo.Rio MTR-PRO-2024/06270)
*/
{% if var("encontro_contas_modo") == "_pos_gt" %}
    {{ config(alias=this.name ~ var("encontro_contas_modo")) }}
    select
        quinzena,
        parse_date("%m/%d/%Y", data_inicio_quinzena) as data_inicio_quinzena,
        parse_date("%m/%d/%Y", data_final_quinzena) as data_final_quinzena,
        consorcio_rdo,
        if(
            length(servico_tratado_rdo) < 3,
            lpad(servico_tratado_rdo, 3, "0"),
            servico_tratado_rdo
        ) as servico_tratado_rdo,
        if(length(linha_rdo) < 3, lpad(linha_rdo, 3, "0"), linha_rdo) as linha_rdo,
        tipo_servico_rdo,
        ordem_servico_rdo,
        quantidade_dias_rdo,
        safe_cast(
            replace(
                regexp_replace(receita_tarifaria_aferida_rdo, r"[^\d,-]", ""), ",", "."
            ) as float64
        ) as receita_tarifaria_aferida_rdo,
        justificativa,
        acao,
        case
            when
                justificativa = "SVA665 a partir de 06/2022, SVB665 a partir de 08/2022"
                and acao = "Considerar como SVB no período indicado"
            then "SVB665"
            else regexp_extract(acao, "(?:[A-Z]+|)[0-9]+")
        end as servico_corrigido_rioonibus,
    from
        {{
            source(
                "projeto_subsidio_sppo_encontro_contas",
                "rdo_correcao_rioonibus_servico_quinzena",
            )
        }}
{% else %} {{ config(enabled=false) }}
{% endif %}
