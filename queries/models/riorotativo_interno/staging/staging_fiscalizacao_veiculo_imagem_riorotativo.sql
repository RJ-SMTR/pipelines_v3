{{ config(alias="fiscalizacao_veiculo_imagem") }}

select
    data,
    hora,
    safe_cast(id_fiscalizacao_veiculo as int64) as id_fiscalizacao_veiculo
    json_query(content, '$.imagem_placa') is not null as indicador_imagem_placa,
    json_query(content, '$.imagem1_veiculo') is not null as indicador_imagem1_veiculo,
    json_query(content, '$.imagem2_veiculo') is not null as indicador_imagem2_veiculo,
    datetime(
        safe_cast(json_value(content, '$.data_inclusao') as timestamp),
        "America/Sao_Paulo"
    ) as datetime_inclusao_imagem,
    datetime(
        safe.parse_timestamp('%Y-%m-%d %H:%M:%S%Ez', timestamp_captura),
        "America/Sao_Paulo"
    ) as datetime_captura
from {{ source("source_jae", "fiscalizacao_veiculo_imagem") }}
