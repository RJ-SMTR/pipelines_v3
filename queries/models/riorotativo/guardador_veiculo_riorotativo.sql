{{
    config(
        materialized="table",
        alias="guardador_veiculo",
        enabled=is_current_state_enabled(),
    )
}}

/* backfill com janela antiga desliga o modelo: ver macro is_current_state_enabled */
{% if execute %}
    {% set last_partition_query %}
        select max(data)
        from {{ ref("guardador_veiculo_riorotativo_historico") }}
        where data between date("{{ var('date_range_start') }}") and date("{{ var('date_range_end') }}")
    {% endset %}
    {% set last_partition = run_query(last_partition_query).columns[0].values()[0] %}
    {% if last_partition is none %}
        {{
            exceptions.raise_compiler_error(
                "No guardador_veiculo_riorotativo_historico partitions found between date_range_start and date_range_end"
            )
        }}
    {% endif %}
{% endif %}

select
    id_cliente,
    nome,
    email,
    telefone,
    documento,
    tipo_documento,
    cnpj,
    razao_social,
    nome_fantasia
from {{ ref("guardador_veiculo_riorotativo_historico") }}
where data = date("{{ last_partition }}") and status = "ativo"
