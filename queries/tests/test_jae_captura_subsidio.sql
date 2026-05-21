with
    table_ids as (
        select table_id
        from unnest(['transacao', 'transacao_riocard', 'gps_validador']) as table_id
    ),
    ts as (
        select datetime(timestamp_captura, "America/Sao_Paulo") as timestamp_captura
        from
            unnest(
                generate_timestamp_array(
                    timestamp("{{ var('date_range_start') }}"),
                    timestamp("{{ var('date_range_end') }}"),
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
            and table_id in ('transacao', 'transacao_riocard', 'gps_validador')
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
