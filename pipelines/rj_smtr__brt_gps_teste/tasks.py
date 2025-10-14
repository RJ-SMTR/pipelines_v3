import io
import json
import os
import traceback
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Union

import basedosdados as bd
import pandas as pd
import pendulum
import prefect
import requests
from basedosdados import Storage, Table
from pandas_gbq.exceptions import GenericGBQException
from prefect import task
from utils import get_root_path, build_headers, map_dict_keys
from pytz import timezone

from constants import constants
from utils import (
    create_or_append_table,
)
# from pipelines.utils.pretreatment import transform_to_nested_structure
# from pipelines.utils.secret import get_secret
#trigger cd


def test():
    print("test")
    return

@task
def pre_treatment_br_rj_riodejaneiro_brt_gps(status: dict, timestamp):
    """Basic data treatment for brt gps data. Converts unix time to datetime,
    and apply filtering to stale data that may populate the API response.

    Args:
        status_dict (dict): dict containing the status of the request made to the
        API. Must contain keys: data, timestamp and error

    Returns:
        df: pandas.core.DataFrame containing the treated data.
    """

    # Check previous error
    if status["error"] is not None:
        print("Skipped due to previous error.")
        return {"data": pd.DataFrame(), "error": status["error"]}
    
    print(f"{len(status['data']['veiculos'])} vehicles received from API")   
    print(status['data']['veiculos'][:10])

    error = None
    data = status["data"]["veiculos"]
    timezone = constants.TIMEZONE.value
    print(
        f"""
    Received inputs:
    - timestamp:\n{timestamp}
    - data:\n{data[:10]}"""
    )
    # Create dataframe sctructure
    key_column = "id_veiculo"
    columns = [key_column, "timestamp_gps", "timestamp_captura", "content"]
    df = pd.DataFrame(columns=columns)  # pylint: disable=c0103

    # map_dict_keys change data keys to match project data structure
    df["content"] = [map_dict_keys(piece, constants.GPS_BRT_MAPPING_KEYS.value) for piece in data]
    df[key_column] = [piece[key_column] for piece in data]
    df["timestamp_gps"] = [piece["timestamp_gps"] for piece in data]
    df["timestamp_captura"] = timestamp
    print(f"timestamp captura is:\n{df['timestamp_captura']}")

    # Remove timezone and force it to be config timezone
    print(f"Before converting, timestamp_gps was: \n{df['timestamp_gps']}")
    df["timestamp_gps"] = (
        pd.to_datetime(df["timestamp_gps"], unit="ms").dt.tz_localize("UTC").dt.tz_convert(timezone)
    )
    print(f"After converting the timezone, timestamp_gps is: \n{df['timestamp_gps']}")

    # Filter data for 0 <= time diff <= 1min
    try:
        print(f"Shape antes da filtragem: {df.shape}")

        mask = (df["timestamp_captura"] - df["timestamp_gps"]) <= timedelta(minutes=1)
        df = df[mask]  # pylint: disable=C0103
        print(f"Shape após a filtragem: {df.shape}")
        if df.shape[0] == 0:
            error = ValueError("After filtering, the dataframe is empty!")
            print(error)
            # log_critical(f"@here\nFailed to filter BRT data: \n{error}")
    except Exception:  # pylint: disable = W0703
        err = traceback.format_exc()
        print(err)
        # log_critical(f"@here\nFailed to filter BRT data: \n{err}")

    return {"data": df, "error": error}

@task
def bq_upload(
    dataset_id: str,
    table_id: str,
    filepath: str,
    raw_filepath: str = None,
    partitions: str = None,
    status: dict = None,
):  # pylint: disable=R0913
    """
    Upload raw and treated data to GCS and BigQuery.

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        filepath (str): Path to the saved treated .csv file
        raw_filepath (str, optional): Path to raw .json file. Defaults to None.
        partitions (str, optional): Partitioned directory structure, ie "ano=2022/mes=03/data=01".
        Defaults to None.
        status (dict, optional): Dict containing `error` key from
        upstream tasks.

    Returns:
        None
    """
    print(
        f"""
    Received inputs:
    raw_filepath = {raw_filepath}, type = {type(raw_filepath)}
    treated_filepath = {filepath}, type = {type(filepath)}
    dataset_id = {dataset_id}, type = {type(dataset_id)}
    table_id = {table_id}, type = {type(table_id)}
    partitions = {partitions}, type = {type(partitions)}
    """
    )
    if status["error"] is not None:
        return status["error"]

    error = None

    try:
        # Upload raw to staging
        if raw_filepath:
            st_obj = Storage(table_id=table_id, dataset_id=dataset_id)
            print(
                f"""Uploading raw file to bucket {st_obj.bucket_name} at
                {st_obj.bucket_name}/{dataset_id}/{table_id}"""
            )
            st_obj.upload(
                path=raw_filepath,
                partitions=partitions,
                mode="raw",
                if_exists="replace",
            )

        # Creates and publish table if it does not exist, append to it otherwise
        create_or_append_table(
            dataset_id=dataset_id,
            table_id=table_id,
            path=filepath,
            partitions=partitions,
        )
    except Exception:
        error = traceback.format_exc()
        print(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return error

@task
def create_date_hour_partition(
    timestamp: datetime,
    partition_date_name: str = "data",
    partition_date_only: bool = False,
) -> str:
    """
    Create a date (and hour) Hive partition structure from timestamp.

    Args:
        timestamp (datetime): timestamp to be used as reference
        partition_date_name (str, optional): partition name. Defaults to "data".
        partition_date_only (bool, optional): whether to add hour partition or not

    Returns:
        str: partition string
    """
    partition = f"{partition_date_name}={timestamp.strftime('%Y-%m-%d')}"
    if not partition_date_only:
        partition += f"/hora={timestamp.strftime('%H')}"
    return partition

@task
def create_local_partition_path(
    dataset_id: str, table_id: str, filename: str, partitions: str = None
) -> str:
    """
    Create the full path sctructure which to save data locally before
    upload.

    Args:
        dataset_id (str): dataset_id on BigQuery
        table_id (str): table_id on BigQuery
        filename (str, optional): Single csv name
        partitions (str, optional): Partitioned directory structure, ie "ano=2022/mes=03/data=01"
    Returns:
        str: String path having `mode` and `filetype` to be replaced afterwards,
    either to save raw or staging files.
    """
    data_folder = os.getenv("DATA_FOLDER", "data")
    root = str(get_root_path())
    file_path = f"{root}/{data_folder}/{{mode}}/{dataset_id}/{table_id}"
    file_path += f"/{partitions}/{filename}.{{filetype}}"
    print(f"Creating file path: {file_path}")
    return file_path

@task
def get_current_timestamp(
    timestamp=None, truncate_minute: bool = True, return_str: bool = False
) -> Union[datetime, str]:
    """
    Get current timestamp for flow run.

    Args:
        timestamp: timestamp to be used as reference (optionally, it can be a string)
        truncate_minute: whether to truncate the timestamp to the minute or not
        return_str: if True, the return will be an isoformatted datetime string
                    otherwise it returns a datetime object

    Returns:
        Union[datetime, str]: timestamp for flow run
    """
    if isinstance(timestamp, str):
        timestamp = datetime.fromisoformat(timestamp)
    if not timestamp:
        timestamp = datetime.now(tz=timezone(constants.TIMEZONE.value))
    if truncate_minute:
        timestamp = timestamp.replace(second=0, microsecond=0)
    if return_str:
        timestamp = timestamp.isoformat()

    return timestamp

@task
def get_now_time():
    """
    Returns the HH:MM.
    """
    now = pendulum.now(pendulum.timezone("America/Sao_Paulo"))

    return f"{now.hour}:{f'0{now.minute}' if len(str(now.minute))==1 else now.minute}"

@task
def get_raw(  # pylint: disable=R0912
    url: str,
    headers: str = None,
    filetype: str = "json",
    csv_args: dict = None,
    params: dict = None,
    headers_prefix: str = "brt_api_v2_"
) -> Dict:
    """
    Request data from URL API

    Args:
        url (str): URL to send request
        headers (str, optional): Path to headers guardeded on Vault, if needed.
        filetype (str, optional): Filetype to be formatted (supported only: json, csv and txt)
        csv_args (dict, optional): Arguments for read_csv, if needed
        params (dict, optional): Params to be sent on request

    Returns:
        dict: Containing keys
          * `data` (json): data result
          * `error` (str): catched error, if any. Otherwise, returns None
    """
    data = None
    error = None
    print('Requesting data')
    try:
        if headers is not None:
            headers = build_headers(headers_prefix=headers_prefix)
            # remove from headers, if present
            remove_headers = ["host", "databases"]
            for remove_header in remove_headers:
                if remove_header in list(headers.keys()):
                    del headers[remove_header]

        response = requests.get(
            url,
            headers=headers,
            timeout=constants.MAX_TIMEOUT_SECONDS.value,
            params=params,
        )
        print(f"Request returned status code {response.status_code}")

        if response.ok:  # status code is less than 400
            # if not response.content and url in [
            #     constants.GPS_SPPO_API_BASE_URL_V2.value,
            #     constants.GPS_SPPO_API_BASE_URL.value,
            # ]:
            #     error = "Dados de GPS vazios"

            if filetype == "json":
                data = response.json()

                # todo: move to data check on specfic API # pylint: disable=W0102
                if isinstance(data, dict) and "DescricaoErro" in data.keys():
                    error = data["DescricaoErro"]

            elif filetype in ("txt", "csv"):
                if csv_args is None:
                    csv_args = {}
                data = pd.read_csv(io.StringIO(response.text), **csv_args).to_dict(orient="records")
            else:
                error = "Unsupported raw file extension. Supported only: json, csv and txt"

    except Exception:
        error = traceback.format_exc()
        print(f"[CATCHED] Task failed with error: \n{error}", level="error")

    return {"data": data, "error": error}

@task
def parse_timestamp_to_string(timestamp: datetime, pattern="%Y-%m-%d-%H-%M-%S") -> str:
    """
    Parse timestamp to string pattern.
    """
    return timestamp.strftime(pattern)

@task
def save_raw_local(file_path: str, status: dict, mode: str = "raw") -> str:
    """
    Saves json response from API to .json file.
    Args:
        file_path (str): Path which to save raw file
        status (dict): Must contain keys
          * data: json returned from API
          * error: error catched from API request
        mode (str, optional): Folder to save locally, later folder which to upload to GCS.
    Returns:
        str: Path to the saved file
    """
    _file_path = file_path.format(mode=mode, filetype="json")
    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    if status["error"] is None:
        json.dump(status["data"], Path(_file_path).open("w", encoding="utf-8"))
        print(f"Raw data saved to: {_file_path}")
    return _file_path

@task
def save_treated_local(file_path: str, status: dict, mode: str = "staging") -> str:
    """
    Save treated file to CSV.

    Args:
        file_path (str): Path which to save treated file
        status (dict): Must contain keys
          * `data`: dataframe returned from treatement
          * `error`: error catched from data treatement
        mode (str, optional): Folder to save locally, later folder which to upload to GCS.

    Returns:
        str: Path to the saved file
    """

    print(f"Saving treated data to: {file_path}, {status}")

    _file_path = file_path.format(mode=mode, filetype="csv")

    Path(_file_path).parent.mkdir(parents=True, exist_ok=True)
    if status["error"] is None:
        status["data"].to_csv(_file_path, index=False)
        print(f"Treated data saved to: {_file_path}")

    return _file_path

@task
def upload_logs_to_bq(  # pylint: disable=R0913
    dataset_id: str,
    parent_table_id: str,
    timestamp: str,
    error: str = None,
    previous_error: str = None,
    recapture: bool = False,
):
    """
    Upload execution status table to BigQuery.
    Table is uploaded to the same dataset, named {parent_table_id}_logs.
    If passing status_dict, should not pass timestamp and error.

    Args:
        dataset_id (str): dataset_id on BigQuery
        parent_table_id (str): Parent table id related to the status table
        timestamp (str): ISO formatted timestamp string
        error (str, optional): String associated with error caught during execution
    Returns:
        None
    """
    table_id = parent_table_id + "_logs"
    # Create partition directory
    filename = f"{table_id}_{timestamp.isoformat()}"
    partition = f"data={timestamp.date()}"
    filepath = Path(f"""data/staging/{dataset_id}/{table_id}/{partition}/{filename}.csv""")
    filepath.parent.mkdir(exist_ok=True, parents=True)
    # Create dataframe to be uploaded
    if not error and recapture is True:
        # if the recapture is succeeded, update the column erro
        dataframe = pd.DataFrame(
            {
                "timestamp_captura": [timestamp],
                "sucesso": [True],
                "erro": [f"[recapturado]{previous_error}"],
            }
        )
        print(f"Recapturing {timestamp} with previous error:\n{error}")
    else:
        # not recapturing or error during flow execution
        dataframe = pd.DataFrame(
            {
                "timestamp_captura": [timestamp],
                "sucesso": [error is None],
                "erro": [error],
            }
        )
    # Save data local
    dataframe.to_csv(filepath, index=False)
    # Upload to Storage
    create_or_append_table(
        dataset_id=dataset_id,
        table_id=table_id,
        path=filepath.as_posix(),
        partitions=partition,
    )
    if error is not None:
        raise Exception(f"Pipeline failed with error: {error}")