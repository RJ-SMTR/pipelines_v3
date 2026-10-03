# -*- coding: utf-8 -*-
"""Tasks de captura e coordenação de OS do GTFS."""

from functools import partial

from prefect import runtime, task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__smtr_gtfs import constants
from pipelines.capture__smtr_gtfs.utils import (
    data_index_is_after,
    data_index_sort_key,
    filter_gtfs_table_ids,
    get_os_info,
    get_os_rows,
    get_prepared_gtfs_raw_file,
    next_data_index,
    prepare_gtfs_raw_files,
    read_os_marker,
    redis_state_key,
    write_os_marker,
)
from pipelines.common.capture.default_capture.utils import (
    ShouldCapture,
    SourceCaptureContext,
)
from pipelines.common.utils.redis import get_redis_client


@task(cache_policy=NO_CACHE)
def should_capture_gtfs(env: str) -> ShouldCapture:
    """Seleciona a próxima OS e fornece os dados usados pelo extractor genérico."""
    parameters = runtime.flow_run.parameters
    data_versao_gtfs = parameters.get("data_versao_gtfs")
    upload_from_gcs = parameters.get("upload_from_gcs", False)
    regular_sheet_index = parameters.get("regular_sheet_index")

    redis_client = get_redis_client()
    last_captured_os = read_os_marker(
        redis_client, constants.GTFS_DATASET_ID, "last_captured_os", env
    )
    last_materialized_os = read_os_marker(
        redis_client, constants.GTFS_DATASET_ID, "last_materialized_os", env
    )
    if last_materialized_os is None and last_captured_os is not None:
        write_os_marker(
            redis_client,
            constants.GTFS_DATASET_ID,
            "last_materialized_os",
            last_captured_os,
            env,
        )
        last_materialized_os = last_captured_os

    rows = get_os_rows()
    if last_captured_os is not None and (
        last_materialized_os is None or data_index_is_after(last_captured_os, last_materialized_os)
    ):
        if rows.empty:
            raise ValueError(
                f"A OS capturada {last_captured_os} não está mais na planilha de controle."
            )
        captured_cursor = data_index_sort_key(last_captured_os)
        materialized_cursor = (
            None if last_materialized_os is None else data_index_sort_key(last_materialized_os)
        )
        pending_mask = [
            data_index_sort_key(data_index) <= captured_cursor
            and (
                materialized_cursor is None or data_index_sort_key(data_index) > materialized_cursor
            )
            for data_index in rows["data_index"]
        ]
        pending_os = rows.loc[pending_mask].head(1)
        if pending_os.empty:
            raise ValueError(
                f"Não foi possível localizar uma OS capturada pendente até "
                f"{last_captured_os} na planilha de controle."
            )
        return ShouldCapture(
            value=False,
            payload={
                "pending_materialization": True,
                "data_versao_gtfs": pending_os.iloc[0]["Início da Vigência da OS"],
                "data_index": pending_os.iloc[0]["data_index"],
                "advance_last_materialized": True,
            },
        )

    has_new_os, os_control, data_index, data_versao = get_os_info(
        last_captured_os=last_captured_os,
        data_versao_gtfs=data_versao_gtfs,
        rows=rows,
    )
    if not has_new_os:
        return ShouldCapture(value=False)

    expected_data_index = next_data_index(rows, last_captured_os)
    advance_marker = data_index == expected_data_index
    if last_captured_os is None and last_materialized_os is None and advance_marker:
        if os_control["previous_data_index"] is not None:
            write_os_marker(
                redis_client,
                constants.GTFS_DATASET_ID,
                "last_materialized_os",
                os_control["previous_data_index"],
                env,
            )

    table_parameters = filter_gtfs_table_ids(
        data_versao,
        constants.GTFS_TABLE_CAPTURE_PARAMS.copy(),
    )
    extra_parameters = {
        table_id: {
            "partition_value": data_versao,
            "filename": f"{data_versao}-00-00-00",
            "data_versao_gtfs": data_versao,
            "os_control": os_control,
            "upload_from_gcs": upload_from_gcs,
            "regular_sheet_index": regular_sheet_index,
        }
        for table_id in table_parameters
    }
    capture_datetime_redis = None
    if advance_marker:
        capture_datetime_redis = {
            "key": redis_state_key(constants.GTFS_DATASET_ID, "last_captured_os", env),
            "value": {"last_captured_os": data_index},
        }

    return ShouldCapture(
        value=True,
        payload={
            "source_table_ids": list(table_parameters),
            "extra_parameters": extra_parameters,
            "capture_datetime_redis": capture_datetime_redis,
            "data_versao_gtfs": data_versao,
            "data_index": data_index,
            "advance_last_materialized": (
                advance_marker
                and (
                    last_materialized_os is None
                    or data_index_is_after(data_index, last_materialized_os)
                )
            ),
        },
    )


@task(cache_policy=NO_CACHE)
def create_gtfs_extractor(context: SourceCaptureContext):
    """Prepara o arquivo da tabela e adapta-o ao extractor do flow genérico."""
    raw_filepath = prepare_gtfs_raw_files(context)
    return partial(get_prepared_gtfs_raw_file, raw_filepath=raw_filepath)


@task(cache_policy=NO_CACHE)
def update_last_materialized_os(
    dataset_id: str,
    data_index: str,
    mode: str = "prod",
    advance_marker: bool = True,
) -> None:
    """Avança a última OS materializada depois do tratamento completo."""
    if not advance_marker:
        return

    redis_client = get_redis_client()
    current = read_os_marker(redis_client, dataset_id, "last_materialized_os", mode)
    if current is None or data_index_is_after(data_index, current):
        write_os_marker(redis_client, dataset_id, "last_materialized_os", data_index, mode)
