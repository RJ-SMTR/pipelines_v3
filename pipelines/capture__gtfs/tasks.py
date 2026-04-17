# -*- coding: utf-8 -*-
"""Tasks para captura e tratamento do GTFS"""

import io
import os
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import yaml
from prefect import runtime, task
from prefect.cache_policies import NO_CACHE
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from pipelines.capture__gtfs import constants
from pipelines.capture__gtfs.utils import (
    create_bq_external_table,
    download_controle_os_csv,
    download_file,
    download_xlsx,
    filter_valid_rows,
    get_google_api_service,
    get_upload_storage_blob,
    processa_ordem_servico,
    processa_ordem_servico_faixa_horaria,
    processa_ordem_servico_trajeto_alternativo,
    save_raw_local_func,
    xl_load_workbook_sheetnames,
)
from pipelines.common.utils.fs import get_project_root_path
from pipelines.common.utils.gcp.bigquery import BQTable
from pipelines.common.utils.gcp.storage import Storage
from pipelines.common.utils.pretreatment import (
    create_timestamp_captura,
    transform_to_nested_structure,
)
from pipelines.common.utils.redis import get_redis_client
from pipelines.common.utils.utils import is_running_locally


@task(cache_policy=NO_CACHE)
def get_last_capture_os(dataset_id: str, mode: str = "prod") -> str | None:
    """Busca o último OS capturado no Redis."""
    redis_client = get_redis_client()
    fetch_key = f"{dataset_id}.last_captured_os"
    if mode != "prod":
        fetch_key = f"{mode}.{fetch_key}"

    last_captured_os = redis_client.get(fetch_key)
    if last_captured_os is not None:
        last_captured_os = last_captured_os["last_captured_os"]

    if last_captured_os is not None and "/" in last_captured_os:
        index = last_captured_os.split("_")[1]
        data = datetime.strptime(last_captured_os.split("_")[0], "%d/%m/%Y").strftime("%Y-%m-%d")  # noqa: DTZ007
        last_captured_os = data + "_" + index

    print(f"Last captured os: {last_captured_os}")
    return last_captured_os


@task(cache_policy=NO_CACHE)
def update_last_captured_os(dataset_id: str, data_index: str, mode: str = "prod") -> None:
    """Atualiza o último OS capturado no Redis."""
    redis_client = get_redis_client()
    fetch_key = f"{dataset_id}.last_captured_os"
    if mode != "prod":
        fetch_key = f"{mode}.{fetch_key}"

    last_captured_os = redis_client.get(fetch_key)
    if last_captured_os is not None and "/" in str(last_captured_os):
        index = last_captured_os.split("_")[1]
        data = datetime.strptime(last_captured_os.split("_")[0], "%d/%m/%Y").strftime("%Y-%m-%d")  # noqa: DTZ007
        last_captured_os = data + "_" + index

    if last_captured_os is not None:
        if last_captured_os["last_captured_os"] > data_index:
            return

    redis_client.set(fetch_key, {"last_captured_os": data_index})


@task(cache_policy=NO_CACHE)
def get_os_info(
    last_captured_os: str | None = None, data_versao_gtfs: str | None = None
) -> tuple[bool, dict, str | None, str | None]:
    """Busca informações sobre a OS mais recente disponível."""
    df = download_controle_os_csv(constants.GTFS_CONTROLE_OS_URL)

    flag_new_os = False
    data = {"Início da Vigência da OS": None, "data_index": None}

    if df.empty:
        return flag_new_os, data, data["data_index"], data["Início da Vigência da OS"]

    df = filter_valid_rows(df)

    df["Início da Vigência da OS"] = pd.to_datetime(
        df["Início da Vigência da OS"], format="%d/%m/%Y"
    ).dt.strftime("%Y-%m-%d")

    df["data_index"] = df["Início da Vigência da OS"].astype(str) + "_" + df["index"].astype(str)
    df = df.sort_values(by=["data_index"], ascending=True)

    if data_versao_gtfs is not None:
        df = df.loc[(df["Início da Vigência da OS"] == data_versao_gtfs)]
    elif last_captured_os is None:
        last_captured_os = df["data_index"].max()
        df = df.loc[(df["data_index"] == last_captured_os)]
    else:
        df = df.loc[(df["data_index"] > last_captured_os)]

    print(f"Os info: {df.head()}")
    if len(df) >= 1:
        print("Nova OS encontrada!")
        data = df.to_dict(orient="records")[0]
        flag_new_os = True
        print(f"OS selecionada: {data}")

    return flag_new_os, data, data["data_index"], data["Início da Vigência da OS"]


@task(cache_policy=NO_CACHE)
def filter_gtfs_table_ids(data_versao_gtfs_str: str, gtfs_table_capture_params: dict) -> dict:
    """Filtra os IDs de tabelas GTFS com base na versão dos dados."""
    if data_versao_gtfs_str >= constants.DATA_GTFS_V2_INICIO:
        gtfs_table_capture_params.pop("ordem_servico", None)

    if data_versao_gtfs_str < constants.DATA_GTFS_V4_INICIO:
        gtfs_table_capture_params.pop("ordem_servico_faixa_horaria_sentido", None)

    if data_versao_gtfs_str >= constants.DATA_GTFS_V4_INICIO:
        gtfs_table_capture_params.pop("ordem_servico_faixa_horaria", None)

    if data_versao_gtfs_str < constants.DATA_GTFS_V5_INICIO:
        gtfs_table_capture_params.pop("ordem_servico_trajeto_alternativo_sentido", None)

    if data_versao_gtfs_str >= constants.DATA_GTFS_V5_INICIO:
        gtfs_table_capture_params.pop("ordem_servico_trajeto_alternativo", None)

    return gtfs_table_capture_params


@task(cache_policy=NO_CACHE)
def get_raw_gtfs_files(  # noqa: PLR0913
    os_control: dict,
    local_filepath: list,
    regular_sheet_index: int | None,
    upload_from_gcs: bool,
    data_versao_gtfs: str,
    dict_gtfs: dict,
    env: str,
) -> tuple[list, list]:
    """Baixa e processa os arquivos GTFS brutos."""
    raw_filepaths = []

    print(f"Baixando arquivos: {os_control}")

    if upload_from_gcs:
        print("Baixando arquivos através do GCS")
        file_bytes_os = io.BytesIO(
            get_upload_storage_blob(
                env=env, dataset_id=constants.GTFS_DATASET_ID, filename="os"
            ).download_as_bytes()
        )
        file_bytes_gtfs = io.BytesIO(
            get_upload_storage_blob(
                env=env, dataset_id=constants.GTFS_DATASET_ID, filename="gtfs"
            ).download_as_bytes()
        )
    else:
        print("Baixando arquivos através do Google Drive")
        drive_service = get_google_api_service(service_name="drive", version="v3")
        file_bytes_os = download_xlsx(
            file_link=os_control["Link da OS"], drive_service=drive_service
        )
        file_bytes_gtfs = download_file(
            file_link=os_control["Link do GTFS"], drive_service=drive_service
        )

    sheetnames = xl_load_workbook_sheetnames(file_bytes_os)
    sheetnames = [name for name in sheetnames if "ANEXO" in name]
    print(f"tabs encontradas na planilha Controle OS: {sheetnames}")

    with zipfile.ZipFile(file_bytes_gtfs, "r") as zipped_file:
        for filename in list(dict_gtfs.keys()):
            if filename == "ordem_servico":
                processa_ordem_servico(
                    sheetnames=sheetnames,
                    file_bytes=file_bytes_os,
                    local_filepath=local_filepath,
                    raw_filepaths=raw_filepaths,
                    regular_sheet_index=regular_sheet_index,
                )
            elif "ordem_servico_trajeto_alternativo" in filename:
                processa_ordem_servico_trajeto_alternativo(
                    sheetnames=sheetnames,
                    file_bytes=file_bytes_os,
                    local_filepath=local_filepath,
                    raw_filepaths=raw_filepaths,
                    data_versao_gtfs=data_versao_gtfs,
                )
            elif "ordem_servico_faixa_horaria" in filename:
                processa_ordem_servico_faixa_horaria(
                    sheetnames=sheetnames,
                    file_bytes=file_bytes_os,
                    local_filepath=local_filepath,
                    raw_filepaths=raw_filepaths,
                    data_versao_gtfs=data_versao_gtfs,
                )
            else:
                data = zipped_file.read(filename + ".txt").decode(encoding="utf-8")
                local_file_path = next(filter(lambda x: filename + "/" in x, local_filepath))
                raw_file_path = save_raw_local_func(
                    data=data, filepath=local_file_path, filetype="txt"
                )
                print(f"Saved file: {raw_file_path}")
                raw_filepaths.append(raw_file_path)

    return raw_filepaths, list(dict_gtfs.values())


@task(cache_policy=NO_CACHE)
def transform_gtfs_raw_to_nested(
    raw_filepath: str,
    staging_filepath_template: str,
    primary_key: list,
    timestamp: datetime,
    chunksize: int = 50000,
) -> str:
    """Transforma arquivos raw do GTFS em estrutura aninhada para staging."""
    staging_path = staging_filepath_template.format(mode="staging", filetype="csv")
    Path(staging_path).parent.mkdir(parents=True, exist_ok=True)

    is_first_chunk = True
    for chunk in pd.read_csv(raw_filepath, chunksize=chunksize, dtype=str, on_bad_lines="warn"):
        object_cols = chunk.select_dtypes(include=["object"]).columns
        chunk[object_cols] = chunk[object_cols].apply(lambda x: x.str.strip())

        if "ordem_servico" in raw_filepath and "tipo_os" not in chunk.columns:
            chunk["tipo_os"] = "Regular"

        nested = transform_to_nested_structure(chunk, primary_key)
        nested["timestamp_captura"] = create_timestamp_captura(timestamp)

        if is_first_chunk:
            nested.to_csv(staging_path, index=False, mode="w")
            is_first_chunk = False
        else:
            nested.to_csv(staging_path, index=False, mode="a", header=False)

    print(f"Staging file saved: {staging_path}")
    return staging_path


@task(cache_policy=NO_CACHE)
def upload_raw_data_to_gcs(
    env: str, dataset_id: str, table_id: str, raw_filepath: str, partitions: str
) -> None:
    """Faz upload do arquivo raw para o GCS."""
    Storage(env=env, dataset_id=dataset_id, table_id=table_id).upload_file(
        mode="raw",
        filepath=raw_filepath,
        partition=partitions,
    )


@task(cache_policy=NO_CACHE)
def upload_staging_data_to_gcs(
    env: str, dataset_id: str, table_id: str, staging_filepath: str, partitions: str
) -> None:
    """Faz upload do arquivo staging para o GCS e cria tabela externa no BQ se necessário."""
    staging_dataset_id = f"{dataset_id}_staging"
    tb_obj = BQTable(env=env, dataset_id=staging_dataset_id, table_id=table_id)

    Storage(env=env, dataset_id=dataset_id, table_id=table_id).upload_file(
        mode="staging",
        filepath=staging_filepath,
        partition=partitions,
    )

    if not tb_obj.exists():
        print(f"Tabela {tb_obj.table_full_name} não existe. Criando tabela externa...")
        create_bq_external_table(
            env=env,
            dataset_id=dataset_id,
            table_id=table_id,
            staging_filepath=staging_filepath,
        )


@task(cache_policy=NO_CACHE)
def run_dbt_gtfs(data_versao_gtfs: str) -> None:
    """Executa os modelos dbt do GTFS e planejamento."""
    root_path = get_project_root_path()
    project_dir = root_path / "queries"
    log_dir = f"{project_dir}/logs/{runtime.task_run.id}"

    flags = ["--log-path", log_dir, "--log-level-file", "info", "--log-format", "json"]

    profiles_dir = project_dir / "dev" if is_running_locally() else project_dir
    target_path = project_dir / "target"

    dbt_vars = {
        "data_versao_gtfs": data_versao_gtfs,
        "flow_name": runtime.flow_run.flow_name,
    }
    vars_yaml = yaml.safe_dump(dbt_vars, default_flow_style=True)

    select = (
        f"{constants.GTFS_MATERIALIZACAO_DATASET_ID} "
        f"{constants.PLANEJAMENTO_MATERIALIZACAO_DATASET_ID}"
    )

    invoke = [
        "run",
        "--select",
        select,
        "--exclude",
        constants.GTFS_DBT_EXCLUDE,
        "--vars",
        vars_yaml,
        *flags,
    ]

    print(f"Running DBT Command:\n{' '.join(invoke)}")
    os.environ["DBT_PROJECT_DIR"] = str(project_dir)
    os.environ["DBT_PROFILES_DIR"] = str(profiles_dir)
    os.environ["DBT_TARGET_PATH"] = str(target_path)

    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            target_path=target_path,
        ),
        raise_on_failure=True,
    ).invoke(invoke)

    print("DBT run finalizado com sucesso!")


@task(cache_policy=NO_CACHE)
def run_dbt_tests_gtfs(data_versao_gtfs: str) -> str:
    """Executa os testes dbt do GTFS e planejamento, retornando os logs."""
    root_path = get_project_root_path()
    project_dir = root_path / "queries"
    log_dir = f"{project_dir}/logs/{runtime.task_run.id}"

    flags = ["--log-path", log_dir, "--log-level-file", "info", "--log-format", "json"]

    profiles_dir = project_dir / "dev" if is_running_locally() else project_dir
    target_path = project_dir / "target"

    dbt_vars = {
        "data_versao_gtfs": data_versao_gtfs,
        "flow_name": runtime.flow_run.flow_name,
    }
    vars_yaml = yaml.safe_dump(dbt_vars, default_flow_style=True)

    select = (
        f"{constants.GTFS_MATERIALIZACAO_DATASET_ID} "
        f"{constants.PLANEJAMENTO_MATERIALIZACAO_DATASET_ID}"
    )

    invoke = [
        "test",
        "--select",
        select,
        "--exclude",
        constants.GTFS_DBT_TEST_EXCLUDE,
        "--vars",
        vars_yaml,
        *flags,
    ]

    print(f"Running DBT Tests:\n{' '.join(invoke)}")
    os.environ["DBT_PROJECT_DIR"] = str(project_dir)
    os.environ["DBT_PROFILES_DIR"] = str(profiles_dir)
    os.environ["DBT_TARGET_PATH"] = str(target_path)

    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            target_path=target_path,
        ),
        raise_on_failure=False,
    ).invoke(invoke)

    print("DBT tests finalizados!")
    with (Path(log_dir) / "dbt.log").open("r") as logs:
        return logs.read()
