{{ config(materialized="table") }}

select lote, data_inicio, data_fim, km_referencia_mensal
from
    unnest(
        [
            struct(
                'A2' as lote,
                date '2026-08-16' as data_inicio,
                date '2027-06-15' as data_fim,
                814773.0 as km_referencia_mensal
            ),
            struct('A2', date '2027-06-16', cast(null as date), 1342970.0),
            struct('B1', date '2026-08-16', date '2027-06-15', 985292.0),
            struct('B1', date '2027-06-16', cast(null as date), 1085885.0),
            struct('B2', date '2026-08-16', date '2027-06-15', 264893.0),
            struct('B2', date '2027-06-16', cast(null as date), 511043.0)
        ]
    )
