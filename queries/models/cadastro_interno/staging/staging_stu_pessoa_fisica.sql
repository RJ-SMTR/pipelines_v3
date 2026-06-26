{{ config(alias="pessoa_fisica") }}

select
    data,
    ratr,
    safe_cast(json_value(content, '$.cod_estado') as string) as cod_estado,
    safe_cast(json_value(content, '$.cod_estado_civil') as string) as cod_estado_civil,
    safe_cast(json_value(content, '$.cpf') as string) as cpf,
    safe_cast(json_value(content, '$.cod_escolaridade') as string) as cod_escolaridade,
    safe_cast(json_value(content, '$.situac') as string) as situacao,
    safe_cast(json_value(content, '$.nome') as string) as nome,
    safe_cast(json_value(content, '$.identidade') as string) as identidade,
    safe_cast(json_value(content, '$.idorgao') as string) as orgao_identidade,
    safe_cast(json_value(content, '$.idlocal') as string) as local_identidade,
    date(
        safe_cast(json_value(content, '$.iddtemis') as string)
    ) as data_emissao_identidade,
    safe_cast(json_value(content, '$.inss') as string) as inss,
    safe_cast(
        json_value(content, '$.cod_nacionalidade') as string
    ) as cod_nacionalidade,
    safe_cast(
        json_value(content, '$.cod_estado_naturalidade') as string
    ) as estado_naturalidade,
    safe_cast(json_value(content, '$.naturalidade') as string) as naturalidade,
    safe_cast(json_value(content, '$.cnh') as string) as cnh,
    date(
        safe_cast(json_value(content, '$.dt_primeira_cnh') as string)
    ) as data_primeira_cnh,
    date(
        safe_cast(json_value(content, '$.dt_validade_cnh') as string)
    ) as data_validade_cnh,
    date(
        safe_cast(json_value(content, '$.dt_emissao_cnh') as string)
    ) as data_emissao_cnh,
    safe_cast(json_value(content, '$.cep') as string) as cep,
    safe_cast(json_value(content, '$.municipio') as string) as municipio,
    safe_cast(json_value(content, '$.bairro') as string) as bairro,
    safe_cast(json_value(content, '$.endereco') as string) as endereco,
    safe_cast(json_value(content, '$.numero') as string) as numero,
    safe_cast(json_value(content, '$.complemento') as string) as complemento,
    safe_cast(json_value(content, '$.mae') as string) as mae,
    safe_cast(json_value(content, '$.pai') as string) as pai,
    date(safe_cast(json_value(content, '$.dtnasc') as string)) as data_nascimento,
    safe_cast(json_value(content, '$.sexo') as string) as sexo,
    safe_cast(json_value(content, '$.certidao1') as string) as certidao1,
    safe_cast(json_value(content, '$.certidao2') as string) as certidao2,
    safe_cast(json_value(content, '$.certidao3') as string) as certidao3,
    safe_cast(json_value(content, '$.certidao4') as string) as certidao4,
    date(safe_cast(json_value(content, '$.datacertidoes') as string)) as data_certidoes,
    date(
        safe_cast(json_value(content, '$.dt_criacao_registro') as string)
    ) as data_criacao_registro,
    safe_cast(json_value(content, '$.erro_migracao') as boolean) as erro_migracao,
    safe_cast(json_value(content, '$.categoria_cnh') as string) as categoria_cnh,
    safe_cast(json_value(content, '$.email') as string) as email,
    safe_cast(
        json_value(content, '$.ValidaVenctoCNH') as boolean
    ) as valida_vencimento_cnh,
    safe_cast(json_value(content, '$.codigo_curso') as string) as codigo_curso,
    date(
        safe_cast(json_value(content, '$.data_inicio') as string)
    ) as data_inicio_curso,
    date(safe_cast(json_value(content, '$.data_fim') as string)) as data_fim_curso,
    safe_cast(json_value(content, '$.tipo_curso') as string) as tipo_curso,
    safe_cast(json_value(content, '$.avaliacao') as string) as avaliacao_curso,
    safe_cast(json_value(content, '$.processo') as string) as processo_curso,
    date(
        safe_cast(json_value(content, '$.data_atualizacao') as string)
    ) as data_atualizacao,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S%Ez',
            safe_cast(json_value(content, '$._datetime_execucao_flow') as string)
        ),
        "America/Sao_Paulo"
    ) as datetime_execucao_flow,
    timestamp_captura
from {{ source("source_stu", "pessoa_fisica") }}
