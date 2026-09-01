{{
    config(
        materialized="table",
        alias="guardador_veiculo",
    )
}}

/* backfill com janela antiga desliga o modelo: ver macro is_current_state_enabled */
{% if execute %}
    {% set last_partition_query %}
        select max(data)
        from {{ ref("guardador_veiculo_riorotativo_historico") }}
        where data between date_sub(date("{{ var('date_range_start') }}"), interval 1 day) and date("{{ var('date_range_end') }}")
    {% endset %}
    {% set last_partition = run_query(last_partition_query).columns[0].values()[0] %}
{% endif %}

{% if is_current_state_enabled() %}
    with
        guardador_veiculo_riorotativo_historico as (
            select
                *,
                min(datetime_ultima_atualizacao) over (
                    partition by documento
                ) as datetime_inclusao
            from {{ ref("guardador_veiculo_riorotativo_historico") }}
        ),
    select
        id_cliente,
        nome,
        email,
        telefone,
        documento,
        tipo_documento,
        numero_identificacao,
        cnpj,
        razao_social,
        nome_fantasia,
        datetime_inclusao,
        '{{ var("version") }}' as versao,
        current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao,
        '{{ invocation_id }}' as id_execucao_dbt
    from guardador_veiculo_riorotativo_historico
    where data = date("{{ last_partition }}") and status = "ativo"

{% else %} select * from {{ this }}

{% endif %}
