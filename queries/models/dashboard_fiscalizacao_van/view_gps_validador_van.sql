{{
    config(
        materialized="view",
    )
}}

select
    g.data,
    g.hora,
    g.datetime_gps,
    ap.`Número do Termo` as numero_permissao,
    split(ap.ap, '.')[0] as id_area_planejamento_operador,
    ap.modal as modo_van,
    ap.`Código de linha` as servico,
    ap.`Linha` as descricao_servico,
    concat(ap.`Código de linha`, ': ', ap.`Linha`) as nome_completo_servico,
    g.id_veiculo,
    g.id_validador,
    g.latitude,
    g.longitude,
    g.estado_equipamento
from {{ ref("gps_validador_van") }} g
join {{ ref("operadoras") }} o using (id_operadora)
join
    {{ source("sandbox_cadastro", "operador_van_ap") }} ap
    on lpad(cast(ap.cpf as string), 11, '0') = o.documento
    or o.id_operadora = ap.`Número do Termo`
where
    data between date_sub(
        current_date('America/Sao_Paulo'), interval 7 day
    ) and current_date('America/Sao_Paulo')
    and latitude is not null
    and longitude is not null
    and latitude != 0
    and longitude != 0
