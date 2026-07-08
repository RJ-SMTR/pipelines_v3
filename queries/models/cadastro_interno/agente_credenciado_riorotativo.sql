{{ config(materialized="table") }}

-- depends_on: {{ ref('cliente_cpf_jae') }}
{% if execute %}
    {% set last_partition_query %}
        select max(data)
        from {{ ref("staging_agente_credenciado_riorotativo") }}
        where data between date("{{ var('date_range_start') }}") and date("{{ var('date_range_end') }}")
    {% endset %}
    {% set last_partition = run_query(last_partition_query).columns[0].values()[0] %}
    {% if last_partition is none %}
        {{
            exceptions.raise_compiler_error(
                "No staging_agente_credenciado_riorotativo partitions found between date_range_start and date_range_end"
            )
        }}
    {% endif %}

    {% set staging_partitions_query %}
        select distinct cast(cnpj as int64) as cnpj, cast(documento as int64) as documento
        from {{ ref("staging_agente_credenciado_riorotativo") }}
        where data = date("{{ last_partition }}")
    {% endset %}
    {% set staging_partitions = run_query(staging_partitions_query) %}
    {% set cnpj_partitions = staging_partitions.columns[0].values() | unique | list %}
    {% set cpf_partitions = staging_partitions.columns[1].values() | unique | list %}

    {% set id_cliente_partitions_query %}
        select distinct cast(id_cliente as int64)
        from {{ ref("cliente_cpf_jae") }}
        where cpf_particao in ({{ cpf_partitions | join(", ") if cpf_partitions else "null" }})
    {% endset %}
    {% set id_cliente_partitions = (
        run_query(id_cliente_partitions_query).columns[0].values()
    ) %}
{% endif %}

with
    pessoa_juridica as (
        select cnpj, razao_social, nome_fantasia
        from {{ source("rmi_dados_mestres", "pessoa_juridica") }}
        where
            cnpj_particao
            in ({{ cnpj_partitions | join(", ") if cnpj_partitions else "null" }})
    ),
    cliente as (
        select documento, id_cliente, nome, email, telefone
        from {{ ref("cliente_jae") }}
        where
            id_cliente_particao in (
                {{
                    (
                        id_cliente_partitions | join(", ")
                        if id_cliente_partitions
                        else "null"
                    )
                }}
            )
            and tipo_documento = 'CPF'
    )
select
    c.id_cliente,
    c.nome,
    c.email,
    c.telefone,
    a.documento,
    a.tipo_documento,
    a.cnpj,
    pj.razao_social,
    pj.nome_fantasia
from {{ ref("staging_agente_credenciado_riorotativo") }} as a
left join cliente as c using (documento)
left join pessoa_juridica as pj using (cnpj)
where a.data = date("{{ last_partition }}")
