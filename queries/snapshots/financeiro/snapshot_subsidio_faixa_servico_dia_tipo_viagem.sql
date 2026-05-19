{% snapshot snapshot_subsidio_faixa_servico_dia_tipo_viagem %}

    {{
        config(
            target_schema="financeiro_staging",
            unique_key="concat(data, '-', faixa_horaria_inicio, '-', servico, '-', tipo_viagem, ifnull(concat('-', cast(sentido as string)), ''), ifnull(concat('-', cast(indicador_viagem_dentro_limite as string)), ''), ifnull(concat('-', cast(tecnologia_remunerada as string)), ''),  ifnull(concat('-', cast(tecnologia_apurada as string)), ''), ifnull(concat('-', cast(indicador_ar_condicionado as string)), '') )",
            strategy="timestamp",
            updated_at="timestamp_ultima_atualizacao",
            invalidate_hard_deletes=True,
            partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        )
    }}

    select
        * except (versao),
        timestamp(
            datetime_ultima_atualizacao, "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ ref("subsidio_faixa_servico_dia_tipo_viagem") }}

{% endsnapshot %}
