# -*- coding: utf-8 -*-
from typing import Optional

from prefect import runtime, unmapped
from prefect.tasks import Task

from pipelines.common.capture.default_capture.tasks import (
    create_capture_contexts,
    get_raw_data,
    transform_raw_to_nested_structure,
    upload_raw_file_to_gcs,
    upload_source_data_to_gcs,
)
from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    setup_environment,
)
from pipelines.common.utils.gcp.bigquery import SourceTable


def create_capture_flows_default_tasks(  # noqa: PLR0913
    env: Optional[str],
    sources: list[SourceTable],
    source_table_ids: tuple[str],
    timestamp: str,
    create_extractor_task: Task,
    recapture: bool,
    recapture_days: int,
    recapture_timestamps: list[str],
    extra_parameters: Optional[dict[str, dict]] = None,
    tasks_wait_for: Optional[dict[str, list[Task]]] = None,
):
    """
    Cria o conjunto padrão de tasks para um fluxo de captura.

    Args:
        env (Optional[str]): prod ou dev.
        sources (list[SourceTable]): Lista de objetos SourceTable para captura.
        source_table_ids (tuple[str]): Tupla com os table_ids dos sources a serem capturados.
        timestamp (str): Timestamp de captura.
        create_extractor_task (Task): Task utilizada para criar as tasks de extração.
        recapture (bool): Se a run é recaptura ou não.
        recapture_days (int): Quantidade de dias retroativos usados na recaptura.
        recapture_timestamps (list[str]): Lista de timestamps para serem recapturados.
        tasks_wait_for (Optional[dict[str, list[Task]]]): Mapeamento para adicionar tasks no
            argumento wait_for das tasks retornadas por esta função.
        extra_parameters (Optional[dict[str, dict]]): Parametros extras mapeados no padrão
            {"table_id": {"key": "value", ...}, ...}.

    Returns:
        dict: Dicionário com o retorno das tasks.
    """
    tasks = {}
    tasks_wait_for = tasks_wait_for or {}

    deployment_name = runtime.deployment.name

    tasks["env"] = get_run_env(
        env=env,
        deployment_name=deployment_name,
        wait_for=tasks_wait_for.get("env"),
    )

    tasks["setup_enviroment"] = setup_environment(env=env)

    tasks["timestamp"] = get_scheduled_timestamp(
        timestamp=timestamp,
        wait_for=tasks_wait_for.get("timestamp"),
    )

    tasks["contexts"] = create_capture_contexts(
        env=tasks["env"],
        sources=sources,
        source_table_ids=source_table_ids,
        timestamp=tasks["timestamp"],
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
        extra_parameters=extra_parameters,
        wait_for=tasks_wait_for.get("contexts"),
    )
    contexts = tasks["contexts"]

    data_extractor_future = create_extractor_task.map(
        context=contexts,
        wait_for=unmapped(tasks_wait_for.get("data_extractor")),
    )

    tasks["data_extractor"] = data_extractor_future.result()

    get_raw_future = get_raw_data.map(
        context=contexts,
        data_extractor=tasks["data_extractor"],
        wait_for=unmapped(tasks_wait_for.get("get_raw")),
    )

    tasks["get_raw"] = get_raw_future.result()

    upload_raw_future = upload_raw_file_to_gcs.map(
        context=contexts,
        wait_for=unmapped(
            [
                tasks["get_raw"],
                tasks["setup_enviroment"],
                *tasks_wait_for.get("upload_raw", []),
            ]
        ),
    )

    tasks["upload_raw"] = upload_raw_future.result()

    pretreat_future = transform_raw_to_nested_structure.map(
        context=contexts,
        wait_for=unmapped([tasks["upload_raw"], *tasks_wait_for.get("pretreat", [])]),
    )

    tasks["pretreat"] = pretreat_future.result()

    upload_source_future = upload_source_data_to_gcs.map(
        context=contexts,
        wait_for=unmapped(
            [
                tasks["pretreat"],
                tasks["setup_enviroment"],
                *tasks_wait_for.get("upload_source", []),
            ]
        ),
    )

    tasks["upload_source"] = upload_source_future.result()

    return tasks
