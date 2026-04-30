{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key="id_registro",
        incremental_strategy="merge",
        merge_update_columns=[
            "data",
            "datetime_registro",
            "id_registro",
            "id_veiculo",
            "servico",
            "link_foto",
            "validacao",
        ],
        alias="sppo_registro_agente_verao",
    )
}}

{% if execute %}
    {% set ultima_data_captura_agente_verao = run_query(
        "SELECT MAX(data_captura) FROM "
        ~ ref("sppo_registro_agente_verao_staging")
    )[0][0] %}
{% endif %}

select * except (data_captura)
from {{ ref("sppo_registro_agente_verao_staging") }}
where data_captura = date('{{ ultima_data_captura_agente_verao }}') and validacao = true
qualify row_number() over (partition by id_registro order by datetime_captura desc) = 1
