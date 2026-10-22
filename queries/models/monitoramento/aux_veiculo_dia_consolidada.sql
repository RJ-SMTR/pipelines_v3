{{
    config(
        materialized="ephemeral",
    )
}}

select data, id_veiculo, placa, null as ano_fabricacao, tecnologia, status, indicadores
from {{ ref("sppo_veiculo_dia") }}
where data < date("{{ var('DATA_SUBSIDIO_V15_INICIO') }}")

union all

select
    data,
    id_veiculo,
    placa,
    ano_fabricacao,
    tecnologia,
    if(tipo_veiculo like '%ROD%', "Não licenciado", status) as status,
    indicadores
from {{ ref("veiculo_dia") }}
where modo is null or modo = 'ONIBUS'
