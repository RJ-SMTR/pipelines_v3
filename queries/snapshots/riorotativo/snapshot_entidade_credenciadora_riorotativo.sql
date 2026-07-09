{% snapshot snapshot_entidade_credenciadora_riorotativo %}

    {{
        config(
            target_schema="riorotativo_staging",
            alias="entidade_credenciadora",
            unique_key="cnpj",
            strategy="check",
            check_cols=["razao_social", "nome_fantasia"],
            invalidate_hard_deletes=True,
        )
    }}

    {% if execute %}
        {% set cnpjs_query %}
            select distinct cast(cnpj as int64)
            from {{ ref("agente_credenciado_riorotativo_historico") }}
            where cnpj is not null
        {% endset %}
        {% set cnpjs_entidades = run_query(cnpjs_query).columns[0].values() | list %}
    {% endif %}

    select cnpj, razao_social, nome_fantasia
    from {{ source("rmi_dados_mestres", "pessoa_juridica") }}
    where
        cnpj_particao
        in ({{ cnpjs_entidades | join(", ") if cnpjs_entidades else "null" }})

{% endsnapshot %}
