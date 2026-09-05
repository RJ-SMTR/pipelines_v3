-- =============================================================================
-- Teste de consistência: Autuações negativadas pagas sem confirmação de baixa
-- Verifica se autuações com inclusão e pagamento confirmados possuem baixa confirmada
--
-- Lógica:
-- 1. Identificar autuações com inclusão confirmada e baixa não confirmada
-- 2. Verificar se essas autuações foram pagas na tabela autuacao
-- 3. Se existir pagamento sem confirmação de baixa = inconsistência
-- =============================================================================
{% set autuacao_negativacao = ref("autuacao_negativacao") %}

{% if execute %}
    {% set partitions_query %}
        select distinct concat("'", data, "'") as partition_date
        from {{ autuacao_negativacao }}
        where indicador_nao_inclusao is false
            and data_confirmacao_inclusao is not null
            and data_confirmacao_baixa is null
    {% endset %}
    {% set partitions = run_query(partitions_query).columns[0].values() %}
{% endif %}

with
    autuacoes_negativadas_sem_baixa as (
        select contrato, data as data_autuacao, data_inclusao, data_baixa
        from {{ autuacao_negativacao }}
        where
            {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
            {% else %} false
            {% endif %}
            and indicador_nao_inclusao is false
            and data_confirmacao_inclusao is not null
            and data_confirmacao_baixa is null
    ),

    autuacoes_pagas as (
        select id_auto_infracao, data_pagamento, data as data_autuacao
        from {{ ref("autuacao") }}
        where
            {% if partitions | length > 0 %} data in ({{ partitions | join(", ") }})
            {% else %} false
            {% endif %} and data_pagamento is not null
    )

select
    an.contrato as id_auto_infracao,
    an.data_autuacao,
    an.data_inclusao,
    an.data_baixa,
    ap.data_pagamento,
    'Pagamento registrado sem baixa da negativacao' as tipo_falha
from autuacoes_negativadas_sem_baixa an
inner join
    autuacoes_pagas ap
    on an.contrato = ap.id_auto_infracao
    and an.data_autuacao = ap.data_autuacao
