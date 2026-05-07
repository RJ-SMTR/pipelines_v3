# -*- coding: utf-8 -*-
# from pipelines.treatment__cadastro_veiculo.flow import treatment__cadastro_veiculo


# treatment__cadastro_veiculo(skip_source_check=True, datetime_start="2026-05-04", datetime_end="2026-05-05")


import pandas as pd

from queries.dev.utils import run_dbt_model

# Veja os parâmetros disponíveis da função run_dbt_model em util.py


inicio = "2026-04-25"
fim = "2026-05-06"
# Criar range de datas
run_dates = pd.date_range(start=inicio, end=fim, freq="D")
# Montar string de partitions
partitions = ", ".join([f"date({dt.year}, {dt.month}, {dt.day})" for dt in run_dates])
# # Variáveis para passar ao dbt
vars = {
    "start_date": inicio,
    "end_date": fim,
    "date_range_start": f"{inicio}T00:00:00",
    "date_range_end": f"{fim}T23:59:59",
    "partitions": partitions,
    "data_versao_gtfs": "2025-06-01",
    "feed_start_date": "2025-06-01",
}


run_dbt_model(
    dataset_id="veiculo_dia",
    _vars=vars,
    flags="--target dev --defer --state target-base --favor-state",
)
