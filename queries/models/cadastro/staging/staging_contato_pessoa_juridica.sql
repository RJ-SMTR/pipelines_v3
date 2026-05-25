{{
    config(
        alias="contato_pessoa_juridica",
    )
}}

with
    contato_pessoa_juridica as (
        select
            data,
            safe_cast(nr_seq_contato as string) as nr_seq_contato,
            safe_cast(cd_cliente as string) as cd_cliente,
            timestamp_captura,
            datetime(
                parse_timestamp(
                    '%Y-%m-%dT%H:%M:%S%Ez',
                    safe_cast(json_value(content, '$.DT_INCLUSAO') as string)
                ),
                "America/Sao_Paulo"
            ) as datetime_inclusao,
            safe_cast(json_value(content, '$.NM_CONTATO') as string) as nm_contato,
            safe_cast(json_value(content, '$.NR_RAMAL') as string) as nr_ramal,
            safe_cast(json_value(content, '$.NR_TELEFONE') as string) as nr_telefone,
            safe_cast(json_value(content, '$.TX_EMAIL') as string) as tx_email,
        from
            {{
                source(
                    "br_rj_riodejaneiro_bilhetagem_staging", "contato_pessoa_juridica"
                )
            }}
    ),
    contato_pessoa_juridica_rn as (
        select
            *,
            row_number() over (
                partition by nr_seq_contato, cd_cliente order by timestamp_captura desc
            ) as rn
        from contato_pessoa_juridica
    )
select * except (rn)
from contato_pessoa_juridica_rn
where rn = 1
