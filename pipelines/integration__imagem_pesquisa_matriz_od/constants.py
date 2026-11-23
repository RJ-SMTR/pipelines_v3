# -*- coding: utf-8 -*-
from pathlib import Path

from pipelines.common.utils.fs import get_data_folder_path

_IPEA_MIT_PROJECT_FOLDER_NAME = "mit-ipea-project"
_IPEA_MIT_PROJECT_DATASET_ID = _IPEA_MIT_PROJECT_FOLDER_NAME.replace("-", "_")
IMAGE_LETTERS = ("A", "B", "C", "D")
STOPS_TABLE = "rj-smtr.gtfs.stops"
_DATA_DIR = Path(get_data_folder_path())
JOUNEYS_DATA_FILEPATH = _DATA_DIR / "tmp" / "jouneys.csv"
IMAGE_DIR = _DATA_DIR / "images"
OUT_DIR = _DATA_DIR / "output"
BUCKET_NAMES = {"dev": "rj-smtr-disparo-mensagem-dev", "prod": "rj-smtr-disparo-mensagem"}
GCS_IMAGE_SOURCE_DIR = f"source/{_IPEA_MIT_PROJECT_FOLDER_NAME}/images"
GCS_OUT_DIR = _IPEA_MIT_PROJECT_FOLDER_NAME
VOLUNTEER_SPREAD_SHEET_ID = "1DWAIlpIYpNwzYRMrPYCSZOTof0ZPOfN01KOJ-GgRVNo"
VOLUNTEER_SHEET_NAME = "Respostas ao formulário 1"
JOURNEY_TABLE_NAME = f"rj-smtr-dev.rafael__{_IPEA_MIT_PROJECT_DATASET_ID}.survey_journeys"
REDIS_KEY_SUFIX = _IPEA_MIT_PROJECT_DATASET_ID
REDIS_DICT_KEY = "last_image_generation"
FIRST_TIMESTAMP = "2026-11-01 00:00:00"
