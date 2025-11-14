# -*- coding: utf-8 -*-
from typing import Optional

from prefect import unmapped
from prefect.tasks import Task

from pipelines.default.generic_capture.tasks import (
    create_capture_contexts,
    get_raw_data,
    transform_raw_to_nested_structure,
    upload_raw_file_to_gcs,
    upload_source_data_to_gcs,
)
from pipelines.tasks import get_run_env, get_scheduled_timestamp
from pipelines.utils.gcp.bigquery import SourceTable


def create_capture_flows_default_tasks(  # noqa: PLR0913
    env: Optional[str],
    sources: list[SourceTable],
    timestamp: str,
    create_extractor_task: Task,
    recapture: bool,
    recapture_days: int,
    recapture_timestamps: list[str],
    tasks_wait_for: Optional[dict[str, list[Task]]] = None,
):
    tasks = {}
    tasks_wait_for = tasks_wait_for or {}
    tasks["env"] = get_run_env(
        env=env,
        wait_for=tasks_wait_for.get("env"),
    )
    tasks["timestamp"] = get_scheduled_timestamp(
        timestamp=timestamp,
        wait_for=tasks_wait_for.get("timestamp"),
    )

    tasks["contexts"] = create_capture_contexts(
        env=env,
        sources=sources,
        timestamp=tasks["timestamp"],
        recapture=recapture,
        recapture_days=recapture_days,
        recapture_timestamps=recapture_timestamps,
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
        wait_for=unmapped([tasks["get_raw"], *tasks_wait_for.get("upload_raw", [])]),
    )

    tasks["upload_raw"] = upload_raw_future.result()

    pretreat_future = transform_raw_to_nested_structure.map(
        context=contexts,
        wait_for=unmapped([tasks["upload_raw"], *tasks_wait_for.get("pretreat", [])]),
    )

    tasks["pretreat"] = pretreat_future.result()

    upload_source_future = upload_source_data_to_gcs.map(
        context=contexts,
        wait_for=unmapped([tasks["pretreat"], *tasks_wait_for.get("upload_source", [])]),
    )

    tasks["upload_source"] = upload_source_future.result()

    return tasks
