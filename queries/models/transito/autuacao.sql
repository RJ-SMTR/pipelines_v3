{{
    config(
        materialized="incremental",
        partition_by={"field": "data", "data_type": "date", "granularity": "day"},
        unique_key=["id_autuacao"],
        incremental_strategy="insert_overwrite",
    )
}}

{% set incremental_filter %}
    date(data)
    between date("{{var('date_range_start')}}") and date("{{var('date_range_end')}}")
{% endset %}

{% if execute %}
    {% if is_incremental() %}
        {% set partitions_query %}
            select distinct concat("'", date(data_autuacao), "'") as partition_date
            from
                (
                    select distinct data_autuacao
                    from {{ ref("autuacao_citran") }}
                    where {{ incremental_filter }}
                    union all
                    select distinct data_autuacao
                    from {{ ref("autuacao_serpro") }}
                    where {{ incremental_filter }}
                )
        {% endset %}
        {% set partitions = run_query(partitions_query).columns[0].values() %}
    {% endif %}
{% endif %}

with
    infracoes_renainf as (
        select
            concat(codigo_infracao, desdobramento) as codigo_enquadramento,
            descricao_infracao as tipificacao_resumida,
            amparo_legal
        from {{ source("transito_staging", "infracoes_renainf") }}
    ),
    autuacao_ids as (
        select data, id_autuacao, id_auto_infracao, fonte
        from {{ ref("aux_autuacao_id") }}
        {% if is_incremental() and partitions | length > 0 %}
            where data in ({{ partitions | join(", ") }})
        {% endif %}
    ),
    citran as (
        select
            data_autuacao as data,
            id_auto_infracao,
            datetime(concat(data, ' ', hora, ':00')) as datetime_autuacao,
            data_limite_defesa_previa,
            data_limite_recurso,
            situacao_atual as descricao_situacao_autuacao,
            if(status_infracao != "", status_infracao, null) as status_infracao,
            replace(codigo_enquadramento, '-', '') as codigo_enquadramento,
            safe_cast(
                substr(regexp_extract(pontuacao, r'\d+'), 2) as string
            ) as pontuacao,
            case
                when pontuacao = "03"
                then 'Leve'
                when
                    initcap(regexp_replace(pontuacao, r'\d+', '')) = 'Media'
                    or pontuacao = "04"
                then 'Média'
                when pontuacao = "05"
                then 'Grave'
                when
                    initcap(regexp_replace(pontuacao, r'\d+', '')) = 'Gravissima'
                    or pontuacao = "07"
                then 'Gravíssima'
                else initcap(regexp_replace(pontuacao, r'\d+', ''))
            end as gravidade,
            initcap(tipo_veiculo) as tipo_veiculo,
            if(descricao_veiculo != "", descricao_veiculo, null) as descricao_veiculo,
            safe_cast(null as string) as placa_veiculo,
            safe_cast(null as string) as ano_fabricacao_veiculo,
            safe_cast(null as string) as ano_modelo_veiculo,
            safe_cast(null as string) as cor_veiculo,
            case
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Misto', '0Misto')
                then 'Misto'
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Passageir', '0Passageir', 'Passageiro', '0Passageiro')
                then 'Passageiro'
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Tracao', '0Tracao', 'Tracao')
                then 'Tração'
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Nao Inform', '0Nao Inform', 'Nao Informado', '0Nao Informado')
                then 'Não informado'
                when
                    initcap(regexp_replace(especie_veiculo, r'\d+', ''))
                    in ('Carga', '0Carga')
                then 'Carga'
                else 'Inválido'
            end as especie_veiculo,
            safe_cast(null as string) as uf_infrator,
            safe_cast(null as string) as uf_principal_condutor,
            safe_cast(null as string) as nome_proprietario,
            safe_cast(null as string) as documento_proprietario,
            safe_cast(null as string) as cnh_proprietario,
            if(uf_proprietario != "", uf_proprietario, null) as uf_proprietario,
            if(cep_proprietario != "", cep_proprietario, null) as cep_proprietario,
            safe_cast(null as string) as nome_possuidor_veiculo,
            safe_cast(null as string) as documento_possuidor_veiculo,
            safe_cast(null as string) as cnh_possuidor_veiculo,
            safe_cast(null as string) as endereco_possuidor_veiculo,
            safe_cast(null as string) as bairro_possuidor_veiculo,
            safe_cast(null as string) as municipio_possuidor_veiculo,
            safe_cast(null as string) as cep_possuidor_veiculo,
            safe_cast(null as string) as uf_possuidor_veiculo,
            valor_infracao / 100 as valor_infracao,
            valor_pago / 100 as valor_pago,
            data_pagamento,
            "260010" as id_autuador,
            if(
                descricao_autuador != "", descricao_autuador, null
            ) as descricao_autuador,
            "6001" as id_municipio_autuacao,
            "RIO DE JANEIRO" as descricao_municipio,
            "RJ" as uf_autuacao,
            endereco_autuacao,
            null as tile_autuacao,
            if(
                processo_defesa_autuacao != "00000000"
                and processo_defesa_autuacao != "",
                processo_defesa_autuacao,
                null
            ) as processo_defesa_autuacao,
            if(
                recurso_penalidade_multa != "00000000"
                and recurso_penalidade_multa != "",
                recurso_penalidade_multa,
                null
            ) as recurso_penalidade_multa,
            if(
                processo_troca_real_infrator != "00000000"
                and processo_troca_real_infrator != "",
                processo_troca_real_infrator,
                null
            ) as processo_troca_real_infrator,
            false as status_sne,
            "CITRAN" as fonte,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from {{ ref("autuacao_citran") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
    ),
    serpro as (
        select
            data_autuacao as data,
            id_auto_infracao,
            datetime_autuacao,
            data_limite_defesa_previa,
            data_limite_recurso,
            descricao_situacao_autuacao,
            if(status_infracao != "", status_infracao, null) as status_infracao,
            concat(codigo_enquadramento, codigo_desdobramento) as codigo_enquadramento,
            substr(pontuacao, 1, 1) as pontuacao,
            gravidade,
            initcap(tipo_veiculo) as tipo_veiculo,
            if(descricao_veiculo != "", descricao_veiculo, null) as descricao_veiculo,
            placa_veiculo,
            ano_fabricacao_veiculo,
            ano_modelo_veiculo,
            cor_veiculo,
            especie_veiculo,
            uf_infrator,
            uf_principal_condutor,
            nome_proprietario,
            documento_proprietario,
            cnh_proprietario,
            if(uf_proprietario != "", uf_proprietario, null) as uf_proprietario,
            safe_cast(null as string) as cep_proprietario,
            nome_possuidor_veiculo,
            documento_possuidor_veiculo,
            cnh_possuidor_veiculo,
            endereco_possuidor_veiculo,
            bairro_possuidor_veiculo,
            municipio_possuidor_veiculo,
            cep_possuidor_veiculo,
            uf_possuidor_veiculo,
            valor_infracao,
            valor_pago,
            data_pagamento,
            coalesce(id_autuador, "260010") as id_autuador,
            if(
                descricao_autuador != "", descricao_autuador, null
            ) as descricao_autuador,
            coalesce(id_municipio_autuacao, "6001") as id_municipio_autuacao,
            coalesce(descricao_municipio, "RIO DE JANEIRO") as descricao_municipio,
            coalesce(uf_autuacao, "RJ") as uf_autuacao,
            case
                when logradouro_autuacao is not null
                then
                    rtrim(
                        regexp_replace(
                            concat(
                                logradouro_autuacao,
                                ' ',
                                bairro_autuacao,
                                ' ',
                                complemento
                            ),
                            r'\s+',
                            ' '
                        )
                    )
                when logradouro_rodovia_autuacao is not null
                then logradouro_rodovia_autuacao
                else null
            end as endereco_autuacao,
            null as tile_autuacao,
            processo_defesa_autuacao,
            recurso_penalidade_multa,
            processo_troca_real_infrator,
            if(status_sne = "1.0", true, false) as status_sne,
            "SERPRO" as fonte,
            current_datetime("America/Sao_Paulo") as datetime_ultima_atualizacao
        from {{ ref("autuacao_serpro") }}
        {% if is_incremental() %} where {{ incremental_filter }} {% endif %}
        qualify
            row_number() over (
                partition by id_auto_infracao order by data_atualizacao_dl desc
            )
            = 1
    ),
    autuacao as (
        select *
        from serpro
        union all by name
        select *
        from citran
    ),
    complete_partitions as (
        select *
        from autuacao
        {% if is_incremental() and partitions | length > 0 %}
            union all
            select
                * except (
                    id_autuacao,
                    tipificacao_resumida,
                    amparo_legal,
                    versao,
                    id_execucao_dbt
                )
            from {{ this }}
            where data in ({{ partitions | join(", ") }})
        {% endif %}
    ),
    final as (
        select
            c.data,
            a.id_autuacao,
            c.id_auto_infracao,
            c.datetime_autuacao,
            c.data_limite_defesa_previa,
            c.data_limite_recurso,
            c.descricao_situacao_autuacao,
            c.status_infracao,
            c.codigo_enquadramento,
            i.tipificacao_resumida,
            c.pontuacao,
            c.gravidade,
            i.amparo_legal,
            c.tipo_veiculo,
            c.descricao_veiculo,
            c.placa_veiculo,
            c.ano_fabricacao_veiculo,
            c.ano_modelo_veiculo,
            c.cor_veiculo,
            c.especie_veiculo,
            c.uf_infrator,
            c.uf_principal_condutor,
            c.nome_proprietario,
            c.documento_proprietario,
            c.cnh_proprietario,
            c.uf_proprietario,
            c.cep_proprietario,
            c.nome_possuidor_veiculo,
            c.documento_possuidor_veiculo,
            c.cnh_possuidor_veiculo,
            c.endereco_possuidor_veiculo,
            c.bairro_possuidor_veiculo,
            c.municipio_possuidor_veiculo,
            c.cep_possuidor_veiculo,
            c.uf_possuidor_veiculo,
            c.valor_infracao,
            c.valor_pago,
            c.data_pagamento,
            c.id_autuador,
            c.descricao_autuador,
            c.id_municipio_autuacao,
            c.descricao_municipio,
            c.uf_autuacao,
            c.endereco_autuacao,
            c.tile_autuacao,
            c.processo_defesa_autuacao,
            c.recurso_penalidade_multa,
            c.processo_troca_real_infrator,
            c.status_sne,
            c.fonte,
            c.datetime_ultima_atualizacao
        from complete_partitions as c
        left join autuacao_ids as a using (data, id_auto_infracao, fonte)
        left join infracoes_renainf as i using (codigo_enquadramento)
        qualify
            row_number() over (
                partition by a.id_autuacao order by c.datetime_ultima_atualizacao desc
            )
            = 1
    )
select *, '{{ var("version") }}' as versao, '{{ invocation_id }}' as id_execucao_dbt
from final
