{% snapshot snapshot_tecnologia_servico %}

    {{
        config(
            target_schema="planejamento_staging",
            unique_key="concat(servico, '-', inicio_vigencia)",
            strategy="check",
            check_cols="all",
            invalidate_hard_deletes=True,
        )
    }}

    select *
    from {{ ref("tecnologia_servico") }}

{% endsnapshot %}
