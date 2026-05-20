{{ config(materialized="view", alias="multa") }}

select
    data,
    safe_cast(serie as string) as serie,
    safe_cast(cm as string) as cm,
    safe_cast(json_value(content, '$.ta') as string) as ta,
    safe_cast(json_value(content, '$.ratr') as string) as ratr,
    safe_cast(json_value(content, '$.linha') as string) as linha,
    trim(safe_cast(json_value(content, '$.ordem') as string)) as ordem,
    safe_cast(json_value(content, '$.placa') as string) as placa,
    safe_cast(json_value(content, '$.termo') as string) as termo,
    safe_cast(json_value(content, '$.valor') as numeric) as valor,
    replace(safe_cast(json_value(content, '$.cod_ap') as string), '.0', '') as id_ap,
    safe_cast(json_value(content, '$.pontos') as numeric) as pontos,
    safe_cast(json_value(content, '$.situac') as string) as situacao,
    safe_cast(json_value(content, '$.tpperm') as string) as tpperm,
    safe_cast(json_value(content, '$.tptran') as string) as tptran,
    safe_cast(json_value(content, '$.emissao') as string) as emissao,
    safe_cast(json_value(content, '$.sentido') as string) as sentido,
    safe_cast(json_value(content, '$.apreensao') as string) as apreensao,
    datetime(
        safe_cast(json_value(content, '$.dt_calculo') as string)
    ) as datetime_calculo,
    datetime(
        safe_cast(json_value(content, '$.dt_ciencia') as string)
    ) as datetime_ciencia,
    datetime(
        safe_cast(json_value(content, '$.dt_usuario') as string)
    ) as datetime_usuario,
    safe_cast(json_value(content, '$.logradouro') as string) as logradouro,
    safe_cast(json_value(content, '$.valor_hist') as numeric) as valor_historico,
    safe_cast(json_value(content, '$.complemento') as string) as complemento,
    safe_cast(json_value(content, '$.data_recred') as string) as data_recred,
    datetime(
        safe_cast(json_value(content, '$.dt_infracao') as string)
    ) as datetime_infracao,
    safe_cast(json_value(content, '$.cod_infracao') as string) as id_infracao,
    safe_cast(json_value(content, '$.des_infracao') as string) as descricao_infracao,
    datetime(
        safe_cast(json_value(content, '$.dt_alteracao') as string)
    ) as datetime_alteracao,
    safe_cast(json_value(content, '$.matr_usuario') as string) as matricula_usuario,
    safe_cast(
        replace(
            safe_cast(json_value(content, '$.reincidencia') as string), '.0', ''
        ) as int64
    ) as reincidencia,
    safe_cast(json_value(content, '$.real_infrator') as string) as real_infrator,
    safe_cast(json_value(content, '$.cod_tipo_moeda') as string) as id_tipo_moeda,
    safe_cast(json_value(content, '$.credito_recred') as numeric) as credito_recred,
    safe_cast(json_value(content, '$.grupo_infracao') as string) as grupo_infracao,
    safe_cast(json_value(content, '$.matr_alteracao') as string) as matricula_alteracao,
    datetime(
        safe_cast(json_value(content, '$.DtNotificacaoAutuacao') as string)
    ) as datetime_notificacao_autuacao,
    datetime(
        safe_cast(json_value(content, '$._datetime_execucao_flow') as string)
    ) as datetime_execucao_flow,
    safe_cast(timestamp_captura as datetime) as datetime_captura
from {{ source("source_stu", "multa") }}
