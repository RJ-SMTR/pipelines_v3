
/*
- Descrição:
Filtragem e tratamento básico de registros de gps.
1. Filtra registros antigos. Remove registros que tem diferença maior
   que 1 minuto entre o timestamp_captura e timestamp_gps.
2. Filtra registros que estão fora de uma caixa que contém a área do
   município de Rio de Janeiro.
*/
with
    box as (
        /* 1. Geometria de caixa que contém a área do município de Rio de Janeiro.*/
        select * from rj-smtr.br_rj_riodejaneiro_geo.limites_geograficos_caixa
    ),
    gps as (
        /* 1. Filtra registros antigos. Remove registros que tem diferença maior
   que 1 minuto entre o timestamp_captura e timestamp_gps.*/
        select *, st_geogpoint(longitude, latitude) posicao_veiculo_geo
        from `rj-smtr`.`br_rj_riodejaneiro_brt_gps`.`brt_registros_desaninhada`
        where
                data between date("2022-01-01T00:00:00") and date(
                    "2022-01-02T01:00:00"
                )
                and timestamp_gps > "2022-01-01T00:00:00"
                and timestamp_gps <= "2022-01-02T01:00:00"
                and datetime_diff(timestamp_captura, timestamp_gps, minute)
                between 0 and 1
    ),
    filtrada as (
        /* 2. Filtra registros que estão fora de uma caixa que contém a área do
   município de Rio de Janeiro.*/
        select
            id_veiculo,
            latitude,
            longitude,
            posicao_veiculo_geo,
            velocidade,
            servico,
            timestamp_gps,
            timestamp_captura,
            data,
            hora,
            row_number() over (partition by id_veiculo, timestamp_gps, servico) rn
        from gps
        where
            st_intersectsbox(
                posicao_veiculo_geo,
                (select min_longitude from box),
                (select min_latitude from box),
                (select max_longitude from box),
                (select max_latitude from box)
            )
    )
select * except (rn), '' as versao
from filtrada
where
    rn = 1

    -- trigger sqlfmt again