select
    id_transacao,
    data,
    datetime_transacao,
    consorcio,
    tipo_transacao,
    valor_pagamento,
    data_ordem,
    id_ordem_pagamento,
    id_ordem_pagamento_consorcio_operador_dia,
    datetime_ultima_atualizacao
from {{ ref("transacao") }}
where
    id_ordem_pagamento_consorcio_operador_dia is not null
    and valor_pagamento > 0
    and modo = 'Van'
    and consorcio in ('STPL', 'STPC', 'TEC')
