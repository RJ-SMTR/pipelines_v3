{% snapshot snapshot_veiculo_licenciamento_dia %}

    {{
        config(
            target_schema="cadastro_staging",
            unique_key="concat(data,'-', data_processamento, '-', id_veiculo,'-',ifnull(placa, ''))",
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
    from {{ ref("veiculo_licenciamento_dia") }}

{% endsnapshot %}
