with
    verificacao as (
        select
            timestamp_captura,
            min(indicador_captura_correta) as indicador_captura_correta
        from {{ source("source_jae", "resultado_verificacao_captura_jae") }}
        where
            data between date("{{ var('date_range_start') }}") and date(
                "{{ var('date_range_end') }}"
            )
            and table_id in ('transacao', 'transacao_riocard', 'gps_validador')
        group by 1
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
    ts_filtrado as (
        select ts.timestamp_captura
        from ts
        left join verificacao using (timestamp_captura)
        qualify
            sum(ifnull(cast(not indicador_captura_correta as integer), 1)) over (
                order by ts.timestamp_captura
            )
            = 0
    ),
    datetime_confiavel as (select max(timestamp_captura) as dt from ts_filtrado)
select dt as datetime_confiavel
from datetime_confiavel
where dt is null or dt < datetime("{{ var('date_range_end') }}")
