{{
    config(
        materialized="table",
    )
}}

with
    rede_ensino as (
        select chave as id_rede_ensino, valor as rede_ensino
        from {{ ref("dicionario_bilhetagem") }}
        where id_tabela = "estudante" and coluna = "id_rede_ensino"
    ),
    escola_jae as (
        select
            id_escola,
            codigo_escola,
            descricao as nome,
            id_cre,
            rede_ensino,
            data_inclusao as datetime_inclusao
        from {{ ref("staging_escola") }}
        left join rede_ensino re using (id_rede_ensino)
    ),
    ies as (
        select id_ies
        from {{ source("br_inep_censo_educacao_superior", "ies") }}
        where sigla_uf = "RJ" and id_municipio = "3304557"
        qualify row_number() over (partition by id_ies order by ano desc) = 1
    ),
    curso as (
        select id_ies, tipo_organizacao_administrativa,
        from {{ source("br_inep_censo_educacao_superior", "curso") }}
        where
            sigla_uf = "RJ"
            and id_municipio = "3304557"
            and tipo_organizacao_administrativa in ("1", "2", "3")
        qualify row_number() over (partition by id_ies order by ano desc) = 1
    ),
    tipo_organizacao_administrativa as (
        select chave, valor
        from {{ source("br_inep_censo_educacao_superior", "dicionario") }}
        where nome_coluna = "tipo_organizacao_administrativa"
    ),
    universidade_publica_basedosdados as (
        select id_ies as codigo_escola, d.valor as tipo_organizacao_administrativa
        from curso as c
        left join ies using (id_ies)
        left join
            tipo_organizacao_administrativa as d
            on c.tipo_organizacao_administrativa = d.chave
    ),
    rede_ensino_atualizado as (
        select
            id_escola,
            ej.codigo_escola,
            ej.nome,
            ej.id_cre,
            case
                when id_escola = '33167222'
                then 'Universidade Pública Federal'
                when ej.rede_ensino = 'Universidade'
                then
                    concat(
                        'Universidade ',
                        ifnull(bd.tipo_organizacao_administrativa, 'Privada')
                    )
                else rede_ensino
            end as rede_ensino,
            ej.datetime_inclusao
        from escola_jae ej
        left join universidade_publica_basedosdados bd using (codigo_escola)
    )
select *
from rede_ensino_atualizado
