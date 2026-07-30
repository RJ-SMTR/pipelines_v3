with
    table_ids as (
        select table_id
        from
            unnest(
                [
                    'movimento_estacionamento_veiculo',
                    'estacionamento_veiculo',
                    'veiculo',
                    'fiscalizacao_veiculo'
                ]
            ) as table_id
    ),
    ts as (
        select datetime(timestamp_captura, "America/Sao_Paulo") as timestamp_captura
        from
            unnest(
                generate_timestamp_array(
                    timestamp("{{ var('date_range_start') }}", "America/Sao_Paulo"),
                    if(
                        date("{{ var('date_range_end') }}")
                        = current_date("America/Sao_Paulo"),
                        greatest(
                            timestamp(
                                "{{ var('date_range_start') }}", "America/Sao_Paulo"
                            ),
                            timestamp_sub(
                                timestamp(
                                    "{{ var('date_range_end') }}", "America/Sao_Paulo"
                                ),
                                interval 1 day
                            )
                        ),
                        timestamp("{{ var('date_range_end') }}", "America/Sao_Paulo")
                    ),
                    interval 1 minute
                )
            ) timestamp_captura
    ),
    ts_table as (select * from ts cross join table_ids),
    verificacao as (
        select timestamp_captura, table_id, indicador_captura_correta
        from {{ source("source_jae", "resultado_verificacao_captura_jae") }}
        where
            data between date("{{ var('date_range_start') }}") and date(
                "{{ var('date_range_end') }}"
            )
            and table_id in (
                'movimento_estacionamento_veiculo',
                'estacionamento_veiculo',
                'veiculo',
                'fiscalizacao_veiculo'
            )
    )
select
    timestamp_captura,
    table_id,
    case
        when verificacao.timestamp_captura is null
        then 'Sem registro'
        else 'Captura divergente'
    end as motivo
from ts_table
left join verificacao using (timestamp_captura, table_id)
where ifnull(not verificacao.indicador_captura_correta, true)
