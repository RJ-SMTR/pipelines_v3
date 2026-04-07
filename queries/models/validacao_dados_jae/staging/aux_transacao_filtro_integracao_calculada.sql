{{
    config(
        materialized="view",
    )
}}

{% set aux_particao_calculo_integracao = ref("aux_particao_calculo_integracao") %}

{% if execute and not flags.FULL_REFRESH %}

    {% set partitions_query %}
        select particao from {{ aux_particao_calculo_integracao }}

    {% endset %}

    {% set partitions = run_query(partitions_query).columns[0].values() %}

{% endif %}

select
    t.data,
    t.id_transacao,
    t.datetime_transacao,
    t.datetime_processamento,
    t.modo,
    t.id_consorcio,
    t.consorcio,
    t.id_servico_jae,
    t.servico_jae,
    t.descricao_servico_jae,
    t.sentido,
    t.id_veiculo,
    t.id_validador,
    t.id_cliente,
    t.hash_cartao,
    t.cadastro_cliente,
    t.produto,
    t.produto_jae,
    t.tipo_transacao_jae,
    t.tipo_transacao,
    t.tipo_usuario,
    t.meio_pagamento,
    t.meio_pagamento_jae,
    t.valor_transacao,
    l.tarifa_ida,
    l.tarifa_volta,
    tp.valor_tarifa,
    if
    (
        t.cadastro_cliente = 'Não Cadastrado', t.hash_cartao, t.id_cliente
    ) as cliente_cartao,
    case
        when t.id_servico_jae = '1358'
        then 'Ônibus BUM'
        when
            t.id_servico_jae in (
                "483", "1106", "486", "484", "307", "485", "492", "1301", "1350", "1354"
            )
        then 'SPPO MNS'
        when modo = 'Van'
        then consorcio
        when
            modo = 'Ônibus'
            and not (
                length(ifnull(regexp_extract(servico_jae, r'[0-9]+'), '')) = 4
                and ifnull(regexp_extract(servico_jae, r'[0-9]+'), '') like '2%'
            )
        then 'SPPO'
        when t.modo = 'BRT' and ifnull(l.tarifa_ida, l.tarifa_volta) > tp.valor_tarifa
        then 'BRT ESP'
        else t.modo
    end as modo_join
from {{ ref("transacao") }} t
left join
    {{ ref("aux_linha_tarifa") }} l
    on t.id_servico_jae = l.cd_linha
    and t.datetime_transacao >= l.dt_inicio_validade
    and (l.data_fim_validade is null or t.datetime_transacao < l.data_fim_validade)
join
    {{ ref("tarifa_publica") }} tp
    on t.data >= tp.data_inicio
    and (t.data <= tp.data_fim or tp.data_fim is null)
where
    t.tipo_transacao != 'Gratuidade'
    and t.tipo_transacao_jae != 'Botoeira'
    and date(t.datetime_processamento) < current_date('America/Sao_Paulo')
    {% if not flags.FULL_REFRESH %}
        {% if partitions | length > 0 %} and t.data in ({{ partitions | join(", ") }})
        {% else %} and data = "2000-01-01"
        {% endif %}
    {% else %} and data >= "2000-01-01"
    {% endif %}
group by all
