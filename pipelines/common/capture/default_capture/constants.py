# -*- coding: utf-8 -*-
"""
Valores constantes flow de captura genérico da rj-smtr
"""

SOURCE_FILETYPE = "csv"
SOURCE_PARQUET_FILETYPE = "parquet"
SOURCE_PARQUET_COMPRESSION_LEVEL = 3
FILENAME_PATTERN = "%Y-%m-%d-%H-%M-%S"

FILEPATH_PATTERN = "{dataset_id}/{table_id}/{partition}/{filename}"
RAW_FILEPATH_PATTERN = f"raw/{FILEPATH_PATTERN}.{{filetype}}"

SOURCE_FILEPATH_PATTERN = f"source/{FILEPATH_PATTERN}.{SOURCE_FILETYPE}"
