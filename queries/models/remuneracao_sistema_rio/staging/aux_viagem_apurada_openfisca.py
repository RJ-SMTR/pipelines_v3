# -*- coding: utf-8 -*-
from datetime import datetime

from pyspark.sql.functions import col
from rio_rac_bus_subsidy import calculate_remuneration, get_rule_version


def model(dbt, session):  # noqa: ARG001 - assinatura do dbt
    dbt.config(materialized="table")

    data_inicio = datetime.fromisoformat(dbt.config.get("date_range_start")).date()
    data_fim = datetime.fromisoformat(dbt.config.get("date_range_end")).date()
    incremental_filter = col("data").between(data_inicio, data_fim)

    viagens = dbt.ref("viagem_valida_classificada").filter(incremental_filter).toPandas()
    planejamento = dbt.ref("servico_oferta_faixa").filter(incremental_filter).toPandas()

    resultado = calculate_remuneration(trips=viagens, schedule=planejamento)
    resultado["versao_regra"] = get_rule_version()
    resultado["id_execucao"] = dbt.config.get("invocation_id")

    print("DEBUG resultado:", resultado.shape)
    print("DEBUG tipos:", {col: str(tipo) for col, tipo in resultado.dtypes.items()})

    colunas_nulas = (
        {col: str(resultado[col].dtype) for col in resultado.columns if resultado[col].isna().all()}
        if not resultado.empty
        else "resultado vazio"
    )
    print("DEBUG colunas totalmente nulas:", colunas_nulas)
    return resultado
