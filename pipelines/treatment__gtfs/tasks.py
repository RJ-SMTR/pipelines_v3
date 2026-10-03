# -*- coding: utf-8 -*-
"""Tasks específicas do tratamento GTFS."""

import os
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_gbq
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment.utils import get_dbt_target
from pipelines.treatment__gtfs import constants


@task(cache_policy=NO_CACHE)
def get_planejamento_materialization_window(
    data_versao_gtfs: str,
    env: str,
    flags: list[str] | None = None,
) -> tuple[str, str, dict]:
    """Calcula a janela do planejamento a partir do feed_end_date materializado."""
    flags = flags or []
    target = flags[flags.index("--target") + 1] if "--target" in flags else None
    if target is None:
        target = get_dbt_target(env)

    project_id = "rj-smtr" if target == "prod" else "rj-smtr-dev"
    dataset_id = constants.GTFS_MATERIALIZACAO_DATASET_ID
    if target == "dev":
        dataset_id = f"{os.environ.get('DBT_USER', 'prefect')}__{dataset_id}"

    feed_info_relation = f"{project_id}.{dataset_id}.feed_info"
    query = f"""
        select feed_end_date
        from `{feed_info_relation}`
        where feed_start_date = '{data_versao_gtfs}'
    """
    result = pandas_gbq.read_gbq(query, project_id=project_id)

    if result.empty:
        raise ValueError(
            f"Feed {data_versao_gtfs} não encontrado em {feed_info_relation} "
            f"(target={target}). Verifique se o modelo feed_info_gtfs materializou "
            "a versão solicitada antes de executar o planejamento diário."
        )

    datetime_start = data_versao_gtfs
    feed_end_date = result["feed_end_date"].iloc[0]

    if feed_end_date is None or pd.isna(feed_end_date):
        today = datetime.now(tz=ZoneInfo(smtr_constants.TIMEZONE)).strftime("%Y-%m-%d")
        datetime_end = max(today, data_versao_gtfs)
        additional_vars = {}
        print(
            f"Feed {data_versao_gtfs} é o mais recente: materializando planejamento de "
            f"{datetime_start} até {datetime_end}"
        )
    else:
        datetime_end = feed_end_date.strftime("%Y-%m-%d")
        additional_vars = {"materializar_periodo_exato": True}
        print(
            f"Retificação do feed {data_versao_gtfs}: materializando calendário de "
            f"{datetime_start} até {datetime_end} e planejamento diário até D+2"
        )

    return datetime_start, datetime_end, additional_vars
