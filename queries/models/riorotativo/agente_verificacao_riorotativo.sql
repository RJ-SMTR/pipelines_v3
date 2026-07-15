/* TODO: reativar quando a captura da entidade de verificação existir */
{{
    config(
        materialized="table",
        alias="agente_verificacao",
        enabled=false,
    )
}}

/* backfill com janela antiga desliga o modelo: ver macro is_current_state_enabled */
{% if execute %}
    {% set last_partition_query %}
        select max(data)
        from {{ ref("agente_verificacao_riorotativo_historico") }}
        where data between date("{{ var('date_range_start') }}") and date("{{ var('date_range_end') }}")
    {% endset %}
    {% set last_partition = run_query(last_partition_query).columns[0].values()[0] %}
    {% if last_partition is none %}
        {{
            exceptions.raise_compiler_error(
                "No agente_verificacao_riorotativo_historico partitions found between date_range_start and date_range_end"
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
    nome_fantasia,
    '{{ var("version") }}' as versao,
    current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
    '{{ invocation_id }}' as id_execucao_dbt
from {{ ref("agente_verificacao_riorotativo_historico") }}
where data = date("{{ last_partition }}") and status = "ativo"
