{{
    config(
        materialized="view",
    )
}}

select
    j.journey_key,
    j.rider_document as cpf,
    v.nome_completo as nome_sobrenome,
    concat(v.ddd_telefone, v.numero_telefone) as telefone,
    concat(
        "https://storage.googleapis.com/rj-smtr-disparo-mensagem-dev/mit-ipea-project/",
        journey_key,
        ".png"
    ) as link_imagem
from {{ ref("survey_journeys") }} j
join
    {{ source("mit_ipea_project_sandbox", "voluntario_disparo_mensagem") }} v
    on j.rider_document = lpad(cpf, 11, '0')
