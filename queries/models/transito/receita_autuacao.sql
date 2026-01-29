{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        incremental_strategy="insert_overwrite",
    )
}}

with
    receita_agregada as (
        select data, ano, mes, sum(valor_arrecadacao) as valor_arrecadacao
        from {{ ref("receita_autuacao_fonte") }}
        group by data, ano, mes
    )

select data, ano, mes, valor_arrecadacao
from receita_agregada
{% if is_incremental() %}
    where
        data between date("{{ var('date_range_start') }}") and date(
            "{{ var('date_range_end') }}"
        )
{% endif %}
