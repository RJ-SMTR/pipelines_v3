from basedosdados import Storage, Table
from basedosdados.upload.datatypes import Datatype
from functools import partial
from google.cloud import bigquery
from pathlib import Path
from typing import Union
from os import environ

def map_dict_keys(data: dict, mapping: dict) -> None:
    """
    Map old keys to new keys in a dict.
    """
    for old_key, new_key in mapping.items():
        data[new_key] = data.pop(old_key)
    return data

# def log_critical(message: str, secret_path: str = constants.CRITICAL_SECRET_PATH.value):
#     """Logs message to critical discord channel specified

#     Args:
#         message (str): Message to post on the channel
#         secret_path (str, optional): Secret path storing the webhook to critical channel.
#         Defaults to constants.CRITICAL_SECRETPATH.value.

#     """
#     url = get_secret(secret_path)["url"]
#     return send_discord_message(message=message, webhook_url=url)

def create_bq_table_schema(
    data_sample_path: Union[str, Path],
) -> list[bigquery.SchemaField]:
    """
    Create the bq schema based on the structure of data_sample_path.

    Args:
        data_sample_path (str, Path): Data sample path to auto complete columns names

    Returns:
        list[bigquery.SchemaField]: The table schema
    """

    data_sample_path = Path(data_sample_path)

    if data_sample_path.is_dir():
        data_sample_path = [
            f for f in data_sample_path.glob("**/*") if f.is_file() and f.suffix == ".csv"
        ][0]

    columns = Datatype(source_format="csv").header(data_sample_path=data_sample_path)

    schema = []
    for col in columns:
        schema.append(bigquery.SchemaField(name=col, field_type="STRING", description=None))
    return schema

def create_bq_external_table(table_obj: Table, path: str, bucket_name: str):
    """Creates an BigQuery External table based on sample data

    Args:
        table_obj (Table): BD Table object
        path (str): Table data local path
        bucket_name (str, Optional): The bucket name where the data is located
    """

    Storage(
        dataset_id=table_obj.dataset_id,
        table_id=table_obj.table_id,
        bucket_name=bucket_name,
    ).upload(
        path=path,
        mode="staging",
        if_exists="replace",
    )

    bq_table = bigquery.Table(table_obj.table_full_name["staging"])
    project_name = table_obj.client["bigquery_prod"].project
    table_full_name = table_obj.table_full_name["prod"].replace(
        project_name, f"{project_name}.{bucket_name}", 1
    )
    bq_table.description = f"staging table for `{table_full_name}`"

    bq_table.external_data_configuration = Datatype(
        dataset_id=table_obj.dataset_id,
        table_id=table_obj.table_id,
        schema=create_bq_table_schema(
            data_sample_path=path,
        ),
        mode="staging",
        bucket_name=bucket_name,
        partitioned=True,
        biglake_connection_id=None,
    ).external_config

    table_obj.client["bigquery_staging"].create_table(bq_table)


def create_or_append_table(
    dataset_id: str,
    table_id: str,
    path: str,
    partitions: str = None,
    bucket_name: str = None,
):
    """Conditionally create table or append data to its relative GCS folder.

    Args:
        dataset_id (str): target dataset_id on BigQuery
        table_id (str): target table_id on BigQuery
        path (str): Path to .csv data file
        partitions (str): partition string.
        bucket_name (str, Optional): The bucket name to save the data.
    """
    table_arguments = {"table_id": table_id, "dataset_id": dataset_id}
    if bucket_name is not None:
        table_arguments["bucket_name"] = bucket_name

    tb_obj = Table(**table_arguments)
    dirpath = path.split(partitions)[0]

    if bucket_name is not None:
        create_func = partial(
            create_bq_external_table,
            table_obj=tb_obj,
            path=dirpath,
            bucket_name=bucket_name,
        )

        append_func = partial(
            Storage(dataset_id=dataset_id, table_id=table_id, bucket_name=bucket_name).upload,
            path=path,
            mode="staging",
            if_exists="replace",
            partitions=partitions,
        )

    else:
        create_func = partial(
            tb_obj.create,
            path=dirpath,
            if_table_exists="pass",
            if_storage_data_exists="replace",
        )

        append_func = partial(
            tb_obj.append,
            filepath=path,
            if_exists="replace",
            timeout=600,
            partitions=partitions,
        )

    if not tb_obj.table_exists("staging"):
        print("Table does not exist in STAGING, creating table...")
        create_func()
        print("Table created in STAGING")
    else:
        print("Table already exists in STAGING, appending to it...")
        append_func()
        print("Appended to table on STAGING successfully.")

def get_root_path() -> Path:
    """
    Returns the root path of the project.
    """
    # try:
    #     import pipelines
    # except ImportError as exc:
    #     raise ImportError("pipelines package not found") from exc
    # root_path = Path(pipelines.__file__).parent.parent
    root_path = Path.cwd().as_posix().split('/')[0]
    print(f"Root path: {root_path}")
    # If the root path is site-packages, we're running in a Docker container. Thus, we
    # need to change the root path to /app
    if str(root_path).endswith("site-packages"):
        root_path = Path("/app")
    return root_path

def build_headers(headers_prefix: str) -> dict:
    """
    Build headers dictionary from a list of fields and a prefix.

    Args:
        headers_fields (list): List of header fields.
        headers_prefix (str): Prefix to be added to each header field.

    Returns:
        dict: Dictionary of headers.
    """
    headers = {}
    for key, value in environ.items():
        if key.startswith(headers_prefix):
            headers[key.replace(headers_prefix, "").lower()] = value
    return headers