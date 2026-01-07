{{
    config(
        alias="estudante",
    )
}}

select
    data,
    datetime(
        parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura), "America/Sao_Paulo"
    ) as timestamp_captura,
    safe_cast(json_value(content, '$.numero_matricula') as string) as numero_matricula,
    safe_cast(json_value(content, '$.codigo_escola') as string) as codigo_escola,
    safe_cast(json_value(content, '$.nome') as string) as nome,
    parse_date(
        '%Y-%m-%d', safe_cast(json_value(content, '$.data_nascimento') as string)
    ) as data_nascimento,
    safe_cast(json_value(content, '$.nome_mae') as string) as nome_mae,
    safe_cast(json_value(content, '$.cpf') as string) as cpf,
    replace(
        safe_cast(json_value(content, '$.cd_cliente') as string), '.0', ''
    ) as cd_cliente,
    datetime(
        parse_timestamp(
            '%Y-%m-%dT%H:%M:%E6S%Ez',
            safe_cast(json_value(content, '$.data_inclusao') as string)
        ),
        'America/Sao_Paulo'
    ) as data_inclusao,
    safe_cast(json_value(content, '$.nome_pai') as string) as nome_pai,
    safe_cast(json_value(content, '$.telefone') as string) as telefone,
    safe_cast(json_value(content, '$.email') as string) as email,
    safe_cast(json_value(content, '$.cre') as string) as cre,
    parse_date(
        '%Y-%m-%d', safe_cast(json_value(content, '$.data_fim_curso') as string)
    ) as data_fim_curso,
from {{ source("source_jae", "estudante") }}
