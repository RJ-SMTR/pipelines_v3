{{ config(materialized="table", tags=["identificacao"]) }}

with
    operadora_jae as (
        select
            ot.cd_operadora_transporte,
            ot.cd_cliente,
            m.modo,
            ot.cd_tipo_modal,
            ot.ds_tipo_modal as modo_jae,
            -- STU considera BRT como Ônibus
            case when ot.cd_tipo_modal = '3' then 'Ônibus' else m.modo end as modo_join,
            ot.in_situacao_atividade,
            case
                when c.tipo_pessoa = 'Física'
                then 'CPF'
                when c.tipo_pessoa = 'Jurídica'
                then 'CNPJ'
            end as tipo_documento,
            c.documento as nr_documento,
            c.nome as nm_cliente,
            cb.cd_agencia,
            cb.cd_tipo_conta,
            cb.nm_banco,
            cb.nr_banco,
            cb.nr_conta
        from {{ ref("staging_operadora_transporte") }} as ot
        join {{ ref("cliente_jae") }} as c on ot.cd_cliente = c.id_cliente
        left join
            {{ ref("staging_conta_bancaria") }} as cb on ot.cd_cliente = cb.cd_cliente
        join {{ ref("modos") }} m on ot.cd_tipo_modal = m.id_modo and m.fonte = "jae"
    ),
    stu_pessoa_juridica as (
        select
            perm_autor,
            cnpj as documento,
            processo,
            id_modo,
            modo as modo_stu,
            tipo_permissao,
            data_registro,
            razao_social as nome_operadora,
            "CNPJ" as tipo_documento
        from {{ ref("staging_operadora_empresa") }}
        where
            perm_autor not in (
                {{
                    var("ids_consorcios").keys() | reject(
                        "equalto", "'229000010'"
                    ) | join(", ")
                }}
            )
    ),
    stu_pessoa_fisica as (
        select
            perm_autor,
            cpf as documento,
            processo,
            id_modo,
            modo as modo_stu,
            tipo_permissao,
            data_registro,
            nome as nome_operadora,
            "CPF" as tipo_documento
        from {{ ref("staging_operadora_pessoa_fisica") }}
    ),
    stu as (
        select s.*, m.modo
        from
            (
                select *
                from stu_pessoa_juridica

                union all

                select *
                from stu_pessoa_fisica
            ) s
        join {{ ref("modos") }} m on s.id_modo = m.id_modo and m.fonte = "stu"
    ),
    cadastro as (
        select
            coalesce(s.perm_autor, j.cd_operadora_transporte) as id_operadora,
            upper(
                regexp_replace(
                    normalize(coalesce(s.nome_operadora, j.nm_cliente), nfd), r"\pM", ''
                )
            ) as operadora_completo,
            s.tipo_permissao as tipo_operadora,
            coalesce(j.modo, s.modo) as modo,
            s.modo_stu,
            j.modo_jae,
            s.processo as id_processo,
            s.data_registro as data_processo,
            coalesce(s.documento, j.nr_documento) as documento,
            coalesce(s.tipo_documento, j.tipo_documento) as tipo_documento,
            s.perm_autor as id_operadora_stu,
            j.cd_operadora_transporte as id_operadora_jae,
            safe_cast(
                j.in_situacao_atividade as boolean
            ) as indicador_operador_ativo_jae,
            j.cd_agencia as agencia,
            j.cd_tipo_conta as tipo_conta,
            j.nm_banco as banco,
            lpad(j.nr_banco, 3, '0') as codigo_banco,
            j.nr_conta as conta
        from stu as s
        full outer join
            operadora_jae as j on s.documento = j.nr_documento and s.modo = j.modo_join
    )
select
    id_operadora,
    modo,
    modo_stu,
    modo_jae,
    case
        when tipo_documento = "CNPJ"
        then operadora_completo
        else regexp_replace(operadora_completo, '[^ ]', '*')
    end as operadora,
    operadora_completo,
    tipo_operadora,
    tipo_documento,
    documento,
    codigo_banco,
    banco,
    agencia,
    tipo_conta,
    conta,
    id_operadora_stu,
    id_operadora_jae,
    id_processo,
    data_processo,
    indicador_operador_ativo_jae
from cadastro
where modo not in ("Escolar", "Táxi")
