# -*- coding: utf-8 -*-
"""
Tasks para o flow treatment__jae_timestamps_divergentes
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas_gbq
from google.cloud import bigquery
from prefect import task

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.jae import constants as jae_constants
from pipelines.control__jae_verificacao_captura import constants as verificacao_captura_constants
from pipelines.treatment__jae_timestamps_divergentes import constants


@task
def get_gaps_from_result_table(
    env: str,
    table_ids: list[str],
    timestamp_start: str,
    timestamp_end: str,
) -> dict:
    """
    Obtém informações sobre timestamps com dados divergentes na captura da Jaé

    Args:
        env (str): dev ou prod
        table_ids (list[str]): Lista de tabelas capturadas a serem verificadas
        timestamp_start (str): Timestamp inicial no formato `YYYY-MM-DD HH:MM:SS`
        timestamp_end (str): Timestamp final no formato `YYYY-MM-DD HH:MM:SS`

    Returns:
        dict: Dicionário onde cada chave é um `table_id` e o valor é outro dicionário com:
            - `"timestamps"` (list[str]): Lista de timestamps das falhas de captura
            - `"flag_has_gaps"` (bool): Indica se houve lacunas de captura
    """

    project_id = smtr_constants.PROJECT_NAME[env]
    dataset_id = f"source_{jae_constants.JAE_SOURCE_NAME}"
    result = {}
    date_filter = (
        f"""
        and data between date('{timestamp_start}') and date('{timestamp_end}')
        and
            timestamp_captura
            between datetime('{timestamp_start}') and datetime('{timestamp_end}')
    """
        if timestamp_start is not None and timestamp_end is not None
        else ""
    )

    query = f"""
        select
            table_id,
            format_datetime('%Y-%m-%d %H:%M:%S', timestamp_captura) as timestamp_captura
        from
            {project_id}.{dataset_id}.{verificacao_captura_constants.RESULTADO_VERIFICACAO_CAPTURA_TABLE_ID}
        where
            not indicador_captura_correta
            and table_id in ({", ".join([f"'{t}'" for t in table_ids])})
            {date_filter}
        """

    df_bq = pandas_gbq.read_gbq(query, project_id=project_id)
    for table_id in table_ids:
        df = df_bq[df_bq["table_id"] == table_id]
        timestamps = df["timestamp_captura"].to_list()
        result[table_id] = {
            "timestamps": timestamps,
            "flag_has_gaps": not df.empty,
        }
    return result


@task
def create_recapture_subflows_params(gaps: dict) -> list[dict]:
    """
    Cria os parâmetros para os subflows de recaptura.

    Args:
        gaps (dict): Gaps identificados para cada tabela de captura.

    Returns:
        list[dict]: Lista de configurações dos subflows de recaptura.
    """
    flows = {}

    for table_id, result in gaps.items():
        if result["flag_has_gaps"]:
            flow = constants.CAPTURE_GAP_TABLES[table_id]["flow"]
            if flow.name in flows.keys():
                flows[flow.name]["params"]["recapture_timestamps"] = list(
                    {*flows[flow.name]["params"]["recapture_timestamps"], *result["timestamps"]}
                )
                flows[flow.name]["params"]["table_ids"].append(table_id)

            else:
                flows[flow.name] = {
                    "flow": flow,
                    "params": {
                        "recapture_timestamps": result["timestamps"],
                        "table_ids": [table_id],
                    },
                }

    for flow_name, value in flows.items():
        timestamps = value["params"]["recapture_timestamps"]

        flows[flow_name]["params"] = [
            {
                "table_ids": value["params"]["table_ids"],
                "recapture": True,
                "recapture_timestamps": timestamps[i : i + 20],
            }
            for i in range(0, len(timestamps), 20)
        ]

    return list(flows.values())


@task
def create_materialization_subflows_params(gaps: dict) -> list[str]:
    """
    Cria os parâmetros para os subflows de materialização.

    Args:
        gaps (dict): Gaps identificados para cada tabela de captura.

    Returns:
        list[dict]: Lista de configurações dos subflows de materialização.
    """
    result = []
    for value in constants.CAPTURE_GAP_SELECTORS:
        timestamps = sorted(
            {timestamp for t in value["capture_tables"] for timestamp in gaps[t]["timestamps"]}
        )

        if len(timestamps) == 0:
            continue

        params = {"flow": value["flow"], "params": []}
        datetime_start = datetime_end = timestamps[0]
        for ts in timestamps:
            if datetime.fromisoformat(ts) >= datetime.fromisoformat(datetime_end) + timedelta(
                hours=2
            ):
                params["params"].append(
                    {
                        "datetime_start": datetime_start,
                        "datetime_end": datetime_end,
                    }
                )

                datetime_start = ts

            datetime_end = ts

        params["params"].append(
            {
                "datetime_start": datetime_start,
                "datetime_end": datetime_end,
            }
        )
        result.append(params)

    return result


@task
def run_updates(env: str, gaps: dict):
    """
    Executa as queries de atualização para as tabelas com gaps.

    Args:
        env (str): prod ou dev.
        gaps (dict): Gaps identificados para cada tabela de captura.
    """
    client = bigquery.Client(project=smtr_constants.PROJECT_NAME[env])
    for treatment in constants.SQL_TREATMENTS:
        timestamps = sorted(
            {timestamp for t in treatment["capture_tables"] for timestamp in gaps[t]["timestamps"]}
        )

        if len(timestamps) > 0:
            query = treatment["sql"].format(
                datetime_start=timestamps[0],
                datetime_end=timestamps[-1],
                project="rj-smtr-dev" if env != "prod" else "rj-smtr",
            )
            print("Executando query:")
            print(query)
            job = client.query(query)
            job.result()

            print(f"{job.num_dml_affected_rows} linhas atualizadas.")


@task
def create_transacao_valor_ordem_params(gaps: dict) -> tuple[bool, dict]:
    """
    Cria os parâmetros para o flow de tratamento da transacao_valor_ordem.

    Args:
        gaps (dict): Gaps identificados para cada tabela de captura.

    Returns:
        tuple[bool, dict]: Indica se há gaps e os parâmetros do flow.
    """
    transacao_gaps = gaps[jae_constants.TRANSACAO_TABLE_ID]

    return transacao_gaps["flag_has_gaps"], {
        "datetime_start": f"{sorted(set(transacao_gaps['timestamps']))[0][:10]} 00:00:00"
    }


@task
def create_verificacao_captura_params(gaps: dict) -> list[dict]:
    """
    Cria os parâmetros para o flow de verificação de captura.

    Args:
        gaps (dict): Gaps identificados para cada tabela de captura.

    Returns:
        list[dict]: Lista de parâmetros agrupados por data de captura.
    """
    result = {}
    current_day = datetime.now(tz=ZoneInfo(smtr_constants.TIMEZONE)).date()
    for table_id, value in gaps.items():
        dates = list({datetime.fromisoformat(t).date() for t in value["timestamps"]})
        for d in dates:
            dates_string = d.isoformat()
            if dates_string not in result.keys():
                result[dates_string] = {
                    "table_ids": [table_id],
                    "retroactive_days": (current_day - d).days,
                }
            else:
                result[dates_string]["table_ids"].append(table_id)

    return list(result.values())
