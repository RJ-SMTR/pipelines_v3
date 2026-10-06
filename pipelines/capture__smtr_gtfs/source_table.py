# -*- coding: utf-8 -*-
"""SourceTable do GTFS no layout legado de staging."""

from datetime import datetime
from zoneinfo import ZoneInfo

from google.cloud import bigquery
from google.cloud.bigquery.external_config import HivePartitioningOptions

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.gcp.bigquery import Dataset, SourceTable
from pipelines.common.utils.gcp.storage import Storage
from pipelines.common.utils.pretreatment import strip_string_columns


class GTFSSourceTable(SourceTable):
    """SourceTable que mantém o layout de staging e a partição Hive do GTFS."""

    def __init__(self, table_id: str, primary_keys: list[str], dataset_id: str) -> None:
        self.staging_dataset_id = f"{dataset_id}_staging"
        super().__init__(
            source_name="gtfs",
            table_id=table_id,
            first_timestamp=datetime(2000, 1, 1, tzinfo=ZoneInfo(smtr_constants.TIMEZONE)),
            flow_folder_name="capture__smtr_gtfs",
            primary_keys=primary_keys,
            pretreatment_reader_args={"dtype": str, "on_bad_lines": "warn"},
            pretreat_funcs=[strip_string_columns],
            partition_date_only=True,
            raw_filetype="txt",
            file_chunk_size=50_000,
            transform_in_chunks=True,
            partition_key="data_versao",
        )
        self.dataset_id = dataset_id
        self.set_env(self.env)

    def set_env(self, env: str):
        super().set_env(env=env)
        self.table_full_name = (
            f"{smtr_constants.PROJECT_NAME[env]}.{self.staging_dataset_id}.{self.table_id}"
        )
        return self

    def _create_table_config(self, sample_filepath: str) -> bigquery.ExternalConfig:
        external_config = bigquery.ExternalConfig("CSV")
        external_config.options.skip_leading_rows = 1
        external_config.options.allow_quoted_newlines = True
        external_config.autodetect = False
        external_config.schema = self._create_table_schema(sample_filepath=sample_filepath)
        external_config.options.field_delimiter = ","
        external_config.options.allow_jagged_rows = False

        source_uri_prefix = f"gs://{self.bucket_name}/staging/{self.dataset_id}/{self.table_id}/"
        external_config.source_uris = [f"{source_uri_prefix}*/*"]
        hive_partitioning = HivePartitioningOptions()
        hive_partitioning.mode = "AUTO"
        hive_partitioning.source_uri_prefix = source_uri_prefix
        external_config.hive_partitioning = hive_partitioning
        return external_config

    def create(self, sample_filepath: str, location: str = "US") -> None:
        Dataset(dataset_id=self.staging_dataset_id, env=self.env, location=location).create()
        table = bigquery.Table(self.table_full_name)
        table.description = f"staging table for `{self.table_full_name}`"
        table.external_data_configuration = self._create_table_config(
            sample_filepath=sample_filepath
        )
        self.client("bigquery").create_table(table)

    def append(self, source_filepath: str, partition: str, if_exists: str = "replace") -> None:
        Storage(
            env=self.env,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
            bucket_names=self.bucket_names,
        ).upload_file(
            mode="staging",
            filepath=source_filepath,
            partition=partition,
            if_exists=if_exists,
        )
