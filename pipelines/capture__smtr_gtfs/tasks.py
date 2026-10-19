# -*- coding: utf-8 -*-
"""Tasks de captura e coordenação de OS do GTFS."""

from functools import partial

from prefect import runtime, task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__smtr_gtfs import constants
from pipelines.capture__smtr_gtfs.utils import (
    data_index_is_after,
    download_gtfs_files,
    extract_gtfs_table,
    filter_gtfs_table_ids,
    get_os_info,
    read_os_marker,
    redis_state_key,
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

    last_captured_os = read_os_marker(
        get_redis_client(), constants.GTFS_DATASET_ID, "last_captured_os", env
    )
    has_new_os, os_control, data_index, data_versao = get_os_info(
        last_captured_os=last_captured_os,
        data_versao_gtfs=data_versao_gtfs,
    )
    if not has_new_os:
        return ShouldCapture(value=False)

    os_filepath, gtfs_filepath = download_gtfs_files(
        os_control=os_control,
        data_versao_gtfs=data_versao,
        upload_from_gcs=upload_from_gcs,
        env=env,
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
            "os_filepath": os_filepath,
            "gtfs_filepath": gtfs_filepath,
        }
        for table_id in table_parameters
    }
    capture_datetime_redis = None
    if last_captured_os is None or data_index_is_after(data_index, last_captured_os):
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
        },
    )


@task(cache_policy=NO_CACHE)
def create_gtfs_extractor(context: SourceCaptureContext):
    """Cria o extractor da tabela GTFS do contexto."""
    return partial(extract_gtfs_table, context=context)
