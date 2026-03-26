{{
    config(
        materialized="incremental",
        partition_by={
            "field": "data_ordem",
            "data_type": "date",
            "granularity": "day",
        },
        unique_key="unique_id",
    )
}}

with
    arquivo_retorno as (
        select * except (rn)
        from
            (
                select
                    *,
                    row_number() over (
                        partition by dataordem, idconsorcio, idoperadora
                        order by timestamp_captura desc
                    ) as rn
                from {{ ref("staging_arquivo_retorno") }}
                {% if is_incremental() %}
                    where
                        date(data) between date("{{var('date_range_start')}}") and date(
                            "{{var('date_range_end')}}"
                        )
                {% endif %}
            )
        where rn = 1
    )
select distinct
    dataordem as data_ordem,
    date(datavencimento) as data_pagamento,
    idconsorcio as id_consorcio,
    idoperadora as id_operadora,
    concat(dataordem, idconsorcio, idoperadora) as unique_id,
    valorrealefetivado as valor_pago
from arquivo_retorno
where ispago = true
