{{
    config(
        materialized="incremental",
        incremental_strategy="merge",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 50000000},
        },
        unique_key="id_cliente_sequencia",
    )
}}

{% set endereco_cliente_jae = ref("endereco_cliente_jae") %}

{% if execute and is_incremental() %}

    {% set cliente_partitions_query %}
        select
            concat(
                "id_cliente_particao between ",
                cast(partition_id as integer),
                " and ",
                cast(partition_id as integer) + 99999
            ) as particao
        from
            `{{ endereco_cliente_jae.database }}.{{ endereco_cliente_jae.schema }}.INFORMATION_SCHEMA.PARTITIONS`
        where
            table_name = "{{ endereco_cliente_jae.identifier }}"
            and partition_id != "__NULL__"
            and datetime(last_modified_time, "America/Sao_Paulo") between datetime("{{var('date_range_start')}}") and datetime_add(datetime("{{var('date_range_end')}}"), interval 30 minute)

    {% endset %}

    {% set cliente_partitions = (
        run_query(cliente_partitions_query).columns[0].values()
    ) %}

{% endif %}

select
    cast(c.documento as integer) as cpf_particao,
    c.documento as cpf,
    e.* except (id_cliente_particao)
from {{ endereco_cliente_jae }} e
join {{ ref("cliente_jae") }} c using (id_cliente_particao)
where
    c.tipo_documento = 'CPF'
    {% if is_incremental() and cliente_partitions | length > 0 %}
        and ({{ cliente_partitions | join("\nor ") }})
    {% endif %}
