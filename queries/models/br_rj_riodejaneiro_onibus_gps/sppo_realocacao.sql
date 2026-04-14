select
    safe_cast(id_veiculo as string) id_veiculo,
    safe_cast(
        datetime(timestamp(datetime_operacao), "America/Sao_Paulo") as datetime
    ) datetime_operacao,
    concat(
        ifnull(regexp_extract(servico, r'[A-Z]+'), ""),
        ifnull(regexp_extract(servico, r'[0-9]+'), "")
    ) as servico,
    safe_cast(
        datetime(timestamp(datetime_entrada), "America/Sao_Paulo") as datetime
    ) as datetime_entrada,
    safe_cast(
        datetime(timestamp(datetime_saida), "America/Sao_Paulo") as datetime
    ) as datetime_saida,
    safe_cast(
        datetime(timestamp(timestamp_processamento), "America/Sao_Paulo") as datetime
    ) as timestamp_processamento,
    safe_cast(
        datetime(timestamp(timestamp_captura), "America/Sao_Paulo") as datetime
    ) as timestamp_captura,
    data,
    hora
from {{ var("sppo_realocacao_staging") }} as t
