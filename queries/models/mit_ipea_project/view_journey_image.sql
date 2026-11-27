{{
    config(
        materialized="view",
    )
}}

select
    service_date as data,
    j.journey_key,
    j.rider_document as cpf,
    upper(
        coalesce(
            nullif(trim(split(trim(v.nome_completo), ' ')[safe_offset(0)]), ''),
            trim(v.nome_completo)
        )
    ) as nome,
    concat(v.ddd_telefone, v.numero_telefone) as telefone,
    case
        when j.boarding_stop_name is not null
        then j.boarding_stop_name
        when j.boarding_mode in ("BRT", "Metrô")
        then initcap(j.boarding_descricao_servico) || " (" || j.boarding_mode || ")"
        else j.boarding_neighborhood
    end as local_embarque,
    case
        when extract(hour from j.boarding_time) between 5 and 11
        then 'de manhã'
        when extract(hour from j.boarding_time) between 12 and 17
        then 'à tarde'
        when extract(hour from j.boarding_time) between 18 and 23
        then 'à noite'
        else 'de madrugada'
    end as turno,
    "A) " || j.stop_name_1 as parada_a,
    "B) " || j.stop_name_2 as parada_b,
    "C) " || j.stop_name_3 as parada_c,
    "D) " || j.stop_name_4 as parada_d,
    concat(
        "https://storage.googleapis.com/rj-smtr-disparo-mensagem/mit-ipea-project/",
        journey_key,
        ".png"
    ) as link_imagem,
    datetime_ultima_atualizacao
from {{ ref("survey_journeys") }} j
join
    {{ source("mit_ipea_project_sandbox", "voluntario_disparo_mensagem") }} v
    on j.rider_document = lpad(cpf, 11, '0')
