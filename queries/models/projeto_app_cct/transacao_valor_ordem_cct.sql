select
    data_ordem,
    data_transacao,
    id_transacao,
    id_operadora,
    valor_transacao_rateio,
    id_ordem_pagamento,
    id_ordem_pagamento_consorcio_dia,
    id_ordem_pagamento_consorcio_operador_dia,
    datetime_ultima_atualizacao
from {{ ref("transacao_valor_ordem") }}
