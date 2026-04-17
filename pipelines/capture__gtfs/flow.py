# -*- coding: utf-8 -*-
"""
Flow de captura e tratamento de dados do GTFS

Captura arquivos GTFS (Ordem de Serviço e tabelas padrão) do Google Drive,
transforma em estrutura aninhada e materializa via dbt.

Common: 2026-04-17
"""

from prefect import flow, runtime

from pipelines.capture__gtfs import constants
from pipelines.capture__gtfs.tasks import (
    filter_gtfs_table_ids,
    get_last_capture_os,
    get_os_info,
    get_raw_gtfs_files,
    run_dbt_gtfs,
    run_dbt_tests_gtfs,
    transform_gtfs_raw_to_nested,
    update_last_captured_os,
    upload_raw_data_to_gcs,
    upload_staging_data_to_gcs,
)
from pipelines.common.tasks import (
    get_run_env,
    get_scheduled_timestamp,
    initialize_sentry,
    setup_environment,
    task_send_discord_message,
)
from pipelines.common.treatment.default_quality_check.tasks import task_dbt_test_notify_discord
from pipelines.common.treatment.default_treatment.utils import DBTTest
from pipelines.common.utils.fs import get_data_folder_path
from pipelines.common.utils.prefect import rename_flow_run


@flow(log_prints=True, flow_run_name=rename_flow_run)
def capture__gtfs(  # noqa: PLR0915
    env=None,
    upload_from_gcs=False,
    materialize_only=False,
    regular_sheet_index=None,
    data_versao_gtfs=None,
):
    """Flow de captura e tratamento dos dados do GTFS."""
    deployment_name = runtime.deployment.name
    env = get_run_env(env=env, deployment_name=deployment_name)
    setup_environment(env=env)
    initialize_sentry(env=env)
    timestamp = get_scheduled_timestamp()

    last_captured_os = None
    if data_versao_gtfs is None:
        last_captured_os = get_last_capture_os(
            dataset_id=constants.GTFS_DATASET_ID,
            mode=env,
        )

    data_versao_gtfs_final = data_versao_gtfs
    data_index = None

    if not materialize_only:
        flag_new_os, os_control, data_index, data_versao_gtfs_task = get_os_info(
            last_captured_os=last_captured_os,
            data_versao_gtfs=data_versao_gtfs,
        )

        if not flag_new_os:
            print("Nenhuma nova OS encontrada. Finalizando flow.")
            return

        data_versao_gtfs_final = data_versao_gtfs_task

        task_send_discord_message(
            message=f"Captura do GTFS {data_versao_gtfs_final} iniciada",
            webhook=constants.GTFS_DISCORD_WEBHOOK,
        )

        partition = f"data_versao={data_versao_gtfs_final}"
        data_root = get_data_folder_path()

        dict_gtfs = filter_gtfs_table_ids(
            data_versao_gtfs_final,
            constants.GTFS_TABLE_CAPTURE_PARAMS.copy(),
        )
        table_ids = list(dict_gtfs.keys())
        pks = list(dict_gtfs.values())

        local_filepaths = [
            f"{data_root}/{{mode}}/{constants.GTFS_DATASET_ID}/{table_id}"
            f"/{partition}/{data_versao_gtfs_final}.{{filetype}}"
            for table_id in table_ids
        ]

        raw_filepaths, _ = get_raw_gtfs_files(
            os_control=os_control,
            local_filepath=local_filepaths,
            regular_sheet_index=regular_sheet_index,
            upload_from_gcs=upload_from_gcs,
            data_versao_gtfs=data_versao_gtfs_final,
            dict_gtfs=dict_gtfs,
            env=env,
        )

        upload_failed = False
        for table_id, raw_fp, pk, local_fp in zip(
            table_ids, raw_filepaths, pks, local_filepaths, strict=False
        ):
            try:
                staging_fp = transform_gtfs_raw_to_nested(
                    raw_filepath=raw_fp,
                    staging_filepath_template=local_fp,
                    primary_key=pk,
                    timestamp=timestamp,
                )

                upload_raw_data_to_gcs(
                    env=env,
                    dataset_id=constants.GTFS_DATASET_ID,
                    table_id=table_id,
                    raw_filepath=raw_fp,
                    partitions=partition,
                )

                upload_staging_data_to_gcs(
                    env=env,
                    dataset_id=constants.GTFS_DATASET_ID,
                    table_id=table_id,
                    staging_filepath=staging_fp,
                    partitions=partition,
                )
            except Exception as e:
                print(f"Erro ao processar tabela {table_id}: {e}")
                upload_failed = True

        if upload_failed:
            task_send_discord_message(
                message=f"Falha na subida dos dados do GTFS {data_versao_gtfs_final}",
                webhook=constants.GTFS_DISCORD_WEBHOOK,
            )
            raise RuntimeError(
                f"Falha no upload de uma ou mais tabelas do GTFS {data_versao_gtfs_final}"
            )

    if data_versao_gtfs_final is None:
        print("Nenhum data_versao_gtfs para materializar. Finalizando flow.")
        return

    dbt_success = True
    try:
        run_dbt_gtfs(data_versao_gtfs=data_versao_gtfs_final)
    except Exception as e:
        dbt_success = False
        print(f"Falha na materialização do GTFS: {e}")

    if data_index is not None:
        update_last_captured_os(
            dataset_id=constants.GTFS_DATASET_ID,
            data_index=data_index,
            mode=env,
        )

    if dbt_success:
        task_send_discord_message(
            message=(
                f"Captura e materialização do GTFS {data_versao_gtfs_final} finalizada com sucesso!"
            ),
            webhook=constants.GTFS_DISCORD_WEBHOOK,
        )
        dbt_test = DBTTest(
            test_select=(
                f"{constants.GTFS_MATERIALIZACAO_DATASET_ID} "
                f"{constants.PLANEJAMENTO_MATERIALIZACAO_DATASET_ID}"
            ),
            exclude=constants.GTFS_DBT_TEST_EXCLUDE,
            test_descriptions=constants.GTFS_DATA_CHECKS_LIST,
        )
        dbt_logs = run_dbt_tests_gtfs(data_versao_gtfs=data_versao_gtfs_final)
        task_dbt_test_notify_discord(
            dbt_test=dbt_test,
            dbt_vars={"data_versao_gtfs": data_versao_gtfs_final},
            dbt_logs=dbt_logs,
            webhook_key=constants.GTFS_DISCORD_WEBHOOK,
            raise_check_error=False,
        )
    else:
        task_send_discord_message(
            message=f"Falha na materialização dos dados do GTFS {data_versao_gtfs_final}",
            webhook=constants.GTFS_DISCORD_WEBHOOK,
        )
        raise RuntimeError(f"DBT run falhou para GTFS {data_versao_gtfs_final}")
