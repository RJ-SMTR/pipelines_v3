# -*- coding: utf-8 -*-
from typing import Any, Optional

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
    initialize_sentry,
    setup_environment,
)
from pipelines.common.utils.gcp.bigquery import SourceTable


def create_capture_flows_default_tasks(  # noqa: PLR0913
    env: Optional[str],
    sources: list[SourceTable],
    timestamp: str,
    create_extractor_task: Task,
    recapture: bool,
    recapture_days: int,
    recapture_timestamps: list[str],
    source_table_ids: Optional[tuple[str]] = None,
    extra_parameters: Optional[dict[str, dict]] = None,
    tasks_wait_for: Optional[dict[str, list[Task]]] = None,
    if_exists_upload: str = "replace",
    should_capture_task: Optional[Task] = None,
    validate_contract_task: Optional[Task] = None,
) -> dict[str, Any]:
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
        should_capture_task (Optional[Task]): Task de gate (opcional) que retorna um
            `ShouldCapture`. Executa após o setup do ambiente e, se `value` for False, interrompe
            a captura cedo. Se None, mantém o comportamento padrão (sempre captura).
        validate_contract_task (Optional[Task]): Task opcional executada após a preservação do
            arquivo raw e antes do pré-tratamento.

    Returns:
        dict: Dicionário com o retorno das tasks. Sempre inclui `should_capture` (bool) e
            `should_capture_result` (ShouldCapture | None). Quando o gate retorna False, o
            dicionário é retornado antes da criação de `timestamp`, `contexts` e tasks de upload.
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

    # initialize sentry for error capturing
    tasks["initialize_sentry"] = initialize_sentry(env=env)

    if should_capture_task is not None:
        result = should_capture_task(
            env=tasks["env"],
            wait_for=[tasks["setup_enviroment"]],
        )
        tasks["should_capture_result"] = result
        tasks["should_capture"] = result.value
        if not result.value:
            print("Gate de captura: fonte sem alteração, pulando captura.")
            return tasks
    else:
        tasks["should_capture_result"] = None
        tasks["should_capture"] = True

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
        if_exists=unmapped(if_exists_upload),
        wait_for=unmapped(
            [
                tasks["get_raw"],
                tasks["setup_enviroment"],
                *tasks_wait_for.get("upload_raw", []),
            ]
        ),
    )

    tasks["upload_raw"] = upload_raw_future.result()

    pretreat_wait_for = [
        tasks["upload_raw"],
        *tasks_wait_for.get("pretreat", []),
    ]
    if validate_contract_task is not None:
        validate_contract_future = validate_contract_task.map(
            context=contexts,
            wait_for=unmapped(
                [
                    tasks["upload_raw"],
                    tasks["setup_enviroment"],
                    *tasks_wait_for.get("validate_contract", []),
                ]
            ),
        )
        tasks["validate_contract"] = validate_contract_future.result()
        pretreat_wait_for.append(tasks["validate_contract"])

    pretreat_future = transform_raw_to_nested_structure.map(
        context=contexts,
        wait_for=unmapped(pretreat_wait_for),
    )

    tasks["pretreat"] = pretreat_future.result()

    upload_source_future = upload_source_data_to_gcs.map(
        context=contexts,
        if_exists=unmapped(if_exists_upload),
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
