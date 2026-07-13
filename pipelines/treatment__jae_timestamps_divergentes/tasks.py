# -*- coding: utf-8 -*-
"""
Tasks para o flow treatment__jae_timestamps_divergentes
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas_gbq
from google.cloud import bigquery
from prefect import task

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.jae import constants as jae_constants
from pipelines.control__jae_verificacao_captura import constants as verificacao_captura_constants
from pipelines.treatment__jae_timestamps_divergentes import constants
from pipelines.treatment__jae_timestamps_divergentes.utils import create_datetime_windows


@task
def get_gaps_from_result_table(
    env: str,
    table_ids: list[str],
    timestamp_start: str,
    timestamp_end: str,
) -> dict[str, list[str]]:
    """
    Obtém os timestamps com dados divergentes na captura da Jaé

    Args:
        env (str): dev ou prod
        table_ids (list[str]): Lista de tabelas capturadas a serem verificadas
        timestamp_start (str): Timestamp inicial no formato `YYYY-MM-DD HH:MM:SS`
        timestamp_end (str): Timestamp final no formato `YYYY-MM-DD HH:MM:SS`

    Returns:
        dict[str, list[str]]: Mapeamento de `table_id` para a lista de timestamps das
            falhas de captura (vazia quando não há lacunas)
    """

    project_id = smtr_constants.PROJECT_NAME[env]
    dataset_id = f"source_{jae_constants.JAE_SOURCE_NAME}"
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
    return {
        table_id: df_bq[df_bq["table_id"] == table_id]["timestamp_captura"].to_list()
        for table_id in table_ids
    }


@task
def create_recapture_subflows_params(gaps: dict[str, list[str]]) -> list[dict]:
    """
    Cria os parâmetros para os subflows de recaptura.

    Tabelas que compartilham o mesmo flow de recaptura têm seus timestamps unidos e
    divididos em lotes de `RECAPTURE_BATCH_SIZE`.

    Args:
        gaps (dict[str, list[str]]): Timestamps das falhas de captura por table_id.

    Returns:
        list[dict]: Lista de configurações dos subflows de recaptura.
    """
    flows = {}

    for table_id, timestamps in gaps.items():
        if not timestamps:
            continue

        spec = constants.GAP_REPAIR_REGISTRY[table_id]
        flow = spec.recapture_flow
        entry = flows.setdefault(
            flow.name,
            {"flow": flow, "timestamps": set(), "source_table_ids": []},
        )
        entry["timestamps"].update(timestamps)
        if spec.recapture_by_table_id:
            entry["source_table_ids"].append(table_id)

    batch_size = constants.RECAPTURE_BATCH_SIZE
    result = []
    for entry in flows.values():
        timestamps = sorted(entry["timestamps"])
        source_table_ids = entry["source_table_ids"]
        result.append(
            {
                "flow": entry["flow"],
                "params": [
                    {
                        "recapture": True,
                        "recapture_timestamps": timestamps[i : i + batch_size],
                        **({"source_table_ids": source_table_ids} if source_table_ids else {}),
                    }
                    for i in range(0, len(timestamps), batch_size)
                ],
            }
        )

    return result


@task
def create_materialization_subflows_params(gaps: dict[str, list[str]]) -> list[dict]:
    """
    Cria os parâmetros para os subflows de materialização.

    Tabelas que compartilham o mesmo flow de materialização têm seus timestamps unidos
    para o cálculo das janelas de datetime.

    Args:
        gaps (dict[str, list[str]]): Timestamps das falhas de captura por table_id.

    Returns:
        list[dict]: Lista de configurações dos subflows de materialização.
    """
    flows = {}
    for table_id, spec in constants.GAP_REPAIR_REGISTRY.items():
        for flow in spec.materialization_flows:
            entry = flows.setdefault(flow.name, {"flow": flow, "timestamps": set()})
            entry["timestamps"].update(gaps[table_id])

    result = []
    for entry in flows.values():
        windows = create_datetime_windows(timestamps=sorted(entry["timestamps"]))
        if windows:
            result.append({"flow": entry["flow"], "params": windows})

    return result


@task
def run_updates(env: str, gaps: dict[str, list[str]]):
    """
    Executa as queries de atualização para as tabelas com gaps.

    Templates SQL compartilhados entre tabelas são executados uma única vez com a união
    dos timestamps.

    Args:
        env (str): prod ou dev.
        gaps (dict[str, list[str]]): Timestamps das falhas de captura por table_id.
    """
    client = bigquery.Client(project=smtr_constants.PROJECT_NAME[env])

    sql_tables = {}
    for table_id, spec in constants.GAP_REPAIR_REGISTRY.items():
        for sql in spec.sql_treatments:
            sql_tables.setdefault(sql, set()).update(gaps[table_id])

    for sql, timestamps in sql_tables.items():
        if not timestamps:
            continue

        sorted_timestamps = sorted(timestamps)
        query = sql.format(
            datetime_start=sorted_timestamps[0],
            datetime_end=sorted_timestamps[-1],
            project=constants.UPDATE_PROJECT_NAME["prod" if env == "prod" else "dev"],
        )
        print("Executando query:")
        print(query)
        job = client.query(query)
        job.result()

        print(f"{job.num_dml_affected_rows} linhas atualizadas.")


@task
def create_downstream_subflows_params(gaps: dict[str, list[str]]) -> list:
    """
    Lista os flows executados após os updates SQL, deduplicados entre as tabelas com gaps.

    Args:
        gaps (dict[str, list[str]]): Timestamps das falhas de captura por table_id.

    Returns:
        list: Flows a serem executados sem parâmetros.
    """
    flows = {}
    for table_id, spec in constants.GAP_REPAIR_REGISTRY.items():
        if gaps[table_id]:
            for flow in spec.downstream_flows:
                flows[flow.name] = flow

    return list(flows.values())


@task
def create_transacao_valor_ordem_params(gaps: dict[str, list[str]]) -> tuple[bool, dict]:
    """
    Cria os parâmetros para o flow de tratamento da transacao_valor_ordem.

    Args:
        gaps (dict[str, list[str]]): Timestamps das falhas de captura por table_id.

    Returns:
        tuple[bool, dict]: Indica se há gaps e os parâmetros do flow.
    """
    transacao_timestamps = gaps[jae_constants.TRANSACAO_TABLE_ID]

    params = (
        {"datetime_start": f"{sorted(set(transacao_timestamps))[0][:10]} 00:00:00"}
        if transacao_timestamps
        else None
    )

    return bool(transacao_timestamps), params


@task
def create_verificacao_captura_params(gaps: dict[str, list[str]]) -> list[dict]:
    """
    Cria os parâmetros para o flow de verificação de captura.

    Args:
        gaps (dict[str, list[str]]): Timestamps das falhas de captura por table_id.

    Returns:
        list[dict]: Lista de parâmetros agrupados por data de captura.
    """
    result = {}
    current_day = datetime.now(tz=ZoneInfo(smtr_constants.TIMEZONE)).date()
    for table_id, timestamps in gaps.items():
        dates = list({datetime.fromisoformat(t).date() for t in timestamps})
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
