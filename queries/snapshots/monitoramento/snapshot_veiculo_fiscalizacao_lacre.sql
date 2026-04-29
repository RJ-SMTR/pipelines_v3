{% snapshot snapshot_veiculo_fiscalizacao_lacre %}

    {{
        config(
            target_schema="monitoramento_staging",
            unique_key="concat(data_inicio_lacre, '-', id_veiculo, '-', placa, '-', id_auto_infracao)",
            strategy="timestamp",
            updated_at="timestamp_ultima_atualizacao",
            invalidate_hard_deletes=True,
        )
    }}

    select
        * except (versao),
        timestamp(
            datetime_ultima_atualizacao, "America/Sao_Paulo"
        ) as timestamp_ultima_atualizacao
    from {{ ref("veiculo_fiscalizacao_lacre") }}

{% endsnapshot %}
