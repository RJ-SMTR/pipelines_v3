{{ config(materialized="view") }}


select distinct
    date(data) as data,
    date(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S',
            regexp_replace(
                safe_cast(json_value(content, '$.auinf_dt_infracao') as string),
                r'\.\d+',
                ''
            )
        ),
        'America/Sao_Paulo'
    ) as data_autuacao,
    auinf_num_auto as id_auto_infracao,
    safe_cast(
        json_value(content, '$.auinf_origem_desc') as string
    ) as origem_auto_infracao,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S',
            regexp_replace(
                safe_cast(json_value(content, '$.auinf_dt_infracao') as string),
                r'\.\d+',
                ''
            )
        ),
        'America/Sao_Paulo'
    ) as datetime_autuacao,
    if(
        json_value(content, '$.auinf_dt_limite_defesa_previa') != '',
        parse_date('%Y-%m-%d', json_value(content, '$.auinf_dt_limite_defesa_previa')),
        null
    ) as data_limite_defesa_previa,
    if(
        json_value(content, '$.auinf_dt_limite_recurso') != '',
        parse_date('%Y-%m-%d', json_value(content, '$.auinf_dt_limite_recurso')),
        null
    ) as data_limite_recurso,
    safe_cast(
        json_value(content, '$.stat_dsc_status_ai') as string
    ) as descricao_situacao_autuacao,
    safe_cast(
        json_value(content, '$.stfu_dsc_status_fluxo_ai') as string
    ) as status_infracao,
    safe_cast(
        json_value(content, '$.htpi_cod_tipo_infracao') as string
    ) as codigo_enquadramento,
    safe_cast(
        json_value(content, '$.htpi_desdobramento') as string
    ) as codigo_desdobramento,
    safe_cast(
        json_value(content, '$.htpi_dsc_tipo_infracao') as string
    ) as tipificacao_resumida,
    safe_cast(json_value(content, '$.htpi_pontosdainfracao') as string) as pontuacao,
    safe_cast(json_value(content, '$.hgrav_descricao') as string) as gravidade,
    safe_cast(json_value(content, '$.htpi_amparo_legal') as string) as amparo_legal,
    safe_cast(
        json_value(content, '$.auinf_infracao_medicao_valor_aferido') as string
    ) as velocidade_aferida,
    safe_cast(
        json_value(content, '$.auinf_infracao_medicao_valor_considerado') as string
    ) as velocidade_considerada,
    safe_cast(
        json_value(content, '$.auinf_infracao_medicao_limite_regulam') as string
    ) as velocidade_regulamentada,
    safe_cast(json_value(content, '$.auinf_vei_tipo') as string) as tipo_veiculo,
    safe_cast(
        json_value(content, '$.auinf_veiculo_marca_modelo_informado') as string
    ) as descricao_veiculo,
    safe_cast(json_value(content, '$.auinf_veiculo_placa') as string) as placa_veiculo,
    safe_cast(
        json_value(content, '$.auinf_veiculo_chassi') as string
    ) as chassi_veiculo,
    safe_cast(
        json_value(content, '$.auinf_veiculo_ano_fabricacao') as string
    ) as ano_fabricacao_veiculo,
    safe_cast(
        json_value(content, '$.auinf_veiculo_ano_modelo') as string
    ) as ano_modelo_veiculo,
    safe_cast(json_value(content, '$.auinf_veiculo_cor_desc') as string) as cor_veiculo,
    safe_cast(
        json_value(content, '$.auinf_veiculo_especie_desc') as string
    ) as especie_veiculo,
    safe_cast(json_value(content, '$.uf_veiculo') as string) as uf_veiculo,
    safe_cast(
        json_value(content, '$.nome_proprietario') as string
    ) as nome_proprietario,
    safe_cast(
        json_value(content, '$.numero_identificacao_proprietario') as string
    ) as documento_proprietario,
    safe_cast(
        json_value(content, '$.numero_cnh_proprietario') as string
    ) as cnh_proprietario,
    safe_cast(json_value(content, '$.uf_cnh_prop') as string) as uf_cnh_proprietario,
    safe_cast(json_value(content, '$.uf_prop_orig') as string) as uf_proprietario,
    safe_cast(
        json_value(
            content, '$.auinf_infrator_condutor_nao_habilitado_numero_doc'
        ) as string
    ) as numero_condutor_nao_habilitado,
    safe_cast(json_value(content, '$.nome_ch_condutor') as string) as nome_condutor,
    safe_cast(
        json_value(content, '$.numero_identificacao_condutor') as string
    ) as numero_identificacao_condutor,
    safe_cast(
        json_value(content, '$.numero_registro_ch_condutor') as string
    ) as cnh_condutor,
    safe_cast(
        json_value(content, '$.uf_princ_cond') as string
    ) as uf_principal_condutor,
    safe_cast(json_value(content, '$.nome_infrator') as string) as nome_infrator,
    safe_cast(json_value(content, '$.cpf_infrator') as string) as cpf_infrator,
    safe_cast(
        json_value(content, '$.auinf_infrator_condutor_habilitado_numero_doc') as string
    ) as cnh_infrator,
    safe_cast(json_value(content, '$.uf_infrator') as string) as uf_infrator,
    safe_cast(
        json_value(content, '$.auinf_infracao_valor') as numeric
    ) as valor_infracao,
    safe_cast(json_value(content, '$.pag_valor') as numeric) as valor_pago,
    if(
        json_value(content, '$.pag_data_pagamento') != '',
        date(
            parse_timestamp(
                '%Y-%m-%d %H:%M:%S',
                regexp_replace(
                    safe_cast(json_value(content, '$.pag_data_pagamento') as string),
                    r'\.\d+',
                    ''
                )
            ),
            'America/Sao_Paulo'
        ),
        null
    ) as data_pagamento,
    safe_cast(json_value(content, '$.ban_codigo_banco') as string) as codigo_banco,
    safe_cast(json_value(content, '$.ban_nome_banco') as string) as nome_banco,
    safe_cast(
        json_value(content, '$.pag_status_pagamento') as string
    ) as status_pagamento,
    safe_cast(
        json_value(content, '$.auinf_codigo_renainf') as string
    ) as codigo_auto_infracao_renainf,
    safe_cast(json_value(content, '$.auinf_id_orgao') as string) as id_autuador,
    safe_cast(
        json_value(content, '$.unaut_dsc_unidade') as string
    ) as descricao_autuador,
    safe_cast(
        json_value(content, '$.usu_num_matricula') as string
    ) as matricula_autuador,
    safe_cast(json_value(content, '$.id_municipio') as string) as id_municipio_autuacao,
    safe_cast(json_value(content, '$.mun_nome') as string) as descricao_municipio,
    safe_cast(json_value(content, '$.uf_sigla') as string) as uf_autuacao,
    safe_cast(
        json_value(content, '$.auinf_local_ende_bairro') as string
    ) as bairro_autuacao,
    safe_cast(json_value(content, '$.auinf_local_ende_cep') as string) as cep_autuacao,
    safe_cast(
        json_value(content, '$.auinf_local_ende_logradouro') as string
    ) as logradouro_autuacao,
    safe_cast(
        json_value(content, '$.auinf_local_ende_numero') as string
    ) as ende_numero_autuacao,
    safe_cast(json_value(content, '$.complemento') as string) as complemento,
    safe_cast(
        json_value(content, '$.auinf_local_rodovia') as string
    ) as logradouro_rodovia_autuacao,
    safe_cast(
        json_value(content, '$.auinf_observacao') as string
    ) as observacao_autuacao,
    safe_cast(
        json_value(content, '$.defp_num_processo') as string
    ) as processo_defesa_autuacao,
    safe_cast(
        json_value(content, '$.rrso_num_processo') as string
    ) as rrso_num_processo,
    safe_cast(json_value(content, '$.cpa_num_processo') as string) as cpa_num_processo,
    safe_cast(
        json_value(content, '$.susp_num_processo') as string
    ) as susp_num_processo,
    safe_cast(
        json_value(content, '$.canc_num_processo') as string
    ) as recurso_penalidade_multa,
    safe_cast(
        json_value(content, '$.ri_proc_nr') as string
    ) as processo_troca_real_infrator,
    safe_cast(
        json_value(content, '$.auinf_veiculo_adesao_sne_indicador') as string
    ) as status_sne,
    safe_cast(
        json_value(content, '$.auinf_veiculo_renavam') as string
    ) as renavam_veiculo,
    safe_cast(
        json_value(content, '$.auinf_veiculo_possuidor_nome') as string
    ) as nome_possuidor_veiculo,
    safe_cast(
        json_value(content, '$.auinf_veiculo_possuidor_numero_identificacao') as string
    ) as documento_possuidor_veiculo,
    safe_cast(
        json_value(content, '$.auinf_veiculo_possuidor_numero_cnh') as string
    ) as cnh_possuidor_veiculo,
    safe_cast(
        json_value(content, '$.auinf_veiculo_adesao_sne_indicador_desc') as string
    ) as descricao_status_sne,
    safe_cast(
        json_value(content, '$.auinf_veiculo_possuidor_ende_completo') as string
    ) as endereco_possuidor_veiculo,
    safe_cast(
        json_value(content, '$.auinf_veiculo_possuidor_ende_bairro') as string
    ) as bairro_possuidor_veiculo,
    safe_cast(
        json_value(content, '$.mun_veiculo_possuidor') as string
    ) as municipio_possuidor_veiculo,
    safe_cast(
        json_value(content, '$.auinf_veiculo_possuidor_ende_cep') as string
    ) as cep_possuidor_veiculo,
    safe_cast(
        json_value(content, '$.uf_veiculo_possuidor') as string
    ) as uf_possuidor_veiculo,
    safe_cast(json_value(content, '$.migrada') as string) as migrada,
    parse_date(
        '%Y-%m-%d', safe_cast(json_value(content, '$.data_atualizacao_dl') as string)
    ) as data_atualizacao_dl,
    datetime(
        parse_timestamp(
            '%Y-%m-%d %H:%M:%S%Ez',
            safe_cast(json_value(content, '$._datetime_execucao_flow') as string)
        ),
        "America/Sao_Paulo"
    ) datetime_execucao_flow,
from {{ source("source_serpro", "autuacao") }}
