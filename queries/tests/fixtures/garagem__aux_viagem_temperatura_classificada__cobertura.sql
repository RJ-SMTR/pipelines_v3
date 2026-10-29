select
    date '2025-10-01' as inicio_vigencia,
    date '2025-10-31' as fim_vigencia,
    st_geogfromtext(
        'POLYGON((-43.25 -22.95, -43.25 -22.94, -43.24 -22.94, -43.24 -22.95, -43.25 -22.95))'
    ) as geometry
