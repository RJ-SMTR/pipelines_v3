{{
    config(
        materialized="table",
        alias="tunel",
    )
}}

select
    parse_date(
        '%d/%m/%Y', nullif(trim(safe_cast(inicio_vigencia as string)), "")
    ) as inicio_vigencia,
    parse_date(
        '%d/%m/%Y', nullif(trim(safe_cast(fim_vigencia as string)), "")
    ) as fim_vigencia,
    safe_cast(regexp_replace(ltrim(cl, '0'), r'\.0$', '') as string) as id_logradouro,
    safe_cast(trim(bairro) as string) as bairro,
    safe_cast(trim(nome_parcial) as string) as nome_parcial,
    safe_cast(trim(completo) as string) as nome_completo,
    safe_cast(
        regexp_replace(ltrim(cod_trecho, '0'), r'\.0$', '') as string
    ) as id_trecho,
    safe_cast(
        regexp_replace(ltrim(cod_tipo_logra, '0'), r'\.0$', '') as string
    ) as id_tipo,
    safe_cast(trim(tipo_logra_ext) as string) as tipo_logradouro,
    safe_cast(trim(tipo_logra_abr) as string) as tipo_logradouro_abreviado,
    safe_cast(
        regexp_replace(ltrim(cod_nobreza, '0'), r'\.0$', '') as string
    ) as id_nobreza,
    safe_cast(trim(nobreza) as string) as nobreza,
    safe_cast(trim(preposicao) as string) as preposicao,
    safe_cast(trim(nome_mapa) as string) as nome_mapa,
    safe_cast(
        regexp_replace(ltrim(cod_bairro, '0'), r'\.0$', '') as string
    ) as id_bairro,
    safe_cast(np_ini_par as int64) as inicio_numero_porta_par,
    safe_cast(np_fin_par as int64) as final_numero_porta_par,
    safe_cast(np_ini_imp as int64) as inicio_numero_porta_impar,
    safe_cast(np_fin_imp as int64) as final_numero_porta_impar,
    safe_cast(hierarquia as string) as hierarquia,
    safe_cast(trim(oneway) as string) as oneway,
    safe_cast(trim(tipo_trecho) as string) as tipo_trecho,
    safe_cast(regexp_replace(ltrim(objectid, '0'), r'\.0$', '') as string) as objectid,
    datetime(
        timestamp_millis(
            safe_cast(regexp_replace(last_edited_date, r'\.0$', '') as int64)
        ),
        "America/Sao_Paulo"
    ) as datetime_ultima_atualizacao,
    safe_cast(geometry_wkt as string) as geometry_wkt,
    st_geogfromtext(geometry) as geometry
from {{ source("planejamento_staging", "tuneis") }}
