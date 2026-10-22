with
    stu_pessoa_juridica as (
        select
            data,
            perm_autor,
            cnpj as documento,
            processo,
            id_modo,
            modo as modo_stu,
            tipo_permissao,
            data_registro,
            razao_social as nome_operadora,
            "CNPJ" as tipo_documento,
            timestamp_captura
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
            data,
            perm_autor,
            cpf as documento,
            processo,
            id_modo,
            modo as modo_stu,
            tipo_permissao,
            data_registro,
            nome as nome_operadora,
            "CPF" as tipo_documento,
            timestamp_captura
        from {{ ref("staging_operadora_pessoa_fisica") }}
    )
select
    datetime("2023-11-09T10:00:00") as datetime_inicio_validade,
    cast(null as datetime) as datetime_fim_validade,
    s.*,
    m.modo
from
    (
        select *
        from stu_pessoa_juridica

        union all

        select *
        from stu_pessoa_fisica
    ) s
join {{ ref("modos") }} m on s.id_modo = m.id_modo and m.fonte = "stu"
