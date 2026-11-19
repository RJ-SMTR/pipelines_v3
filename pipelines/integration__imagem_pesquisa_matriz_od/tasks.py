# -*- coding: utf-8 -*-
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_gbq
from google.cloud import storage
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common import constants as smtr_constants
from pipelines.common.utils.redis import get_redis_client
from pipelines.integration__imagem_pesquisa_matriz_od import constants
from pipelines.integration__imagem_pesquisa_matriz_od.utils import (
    build_journey_image,
    get_volunteers,
    load_font,
    upload_image_to_gcs,
)


@task(cache_policy=NO_CACHE)
def get_date_range(
    env: str, datetime_start: str | None, datetime_end: str | None
) -> tuple[datetime, datetime]:
    if datetime_start is None:
        redis_client = get_redis_client()
        last_run = redis_client.get(f"{env}.{constants.REDIS_KEY_SUFIX}")
        if last_run is None:
            last_run = {constants.REDIS_DICT_KEY: constants.FIRST_TIMESTAMP}
        else:
            last_run = last_run[constants.REDIS_DICT_KEY]

    datetime_start = datetime.fromisoformat(last_run.get(constants.REDIS_DICT_KEY))

    if datetime_end is None:
        datetime_end = datetime.now(tz=ZoneInfo(smtr_constants.TIMEZONE))
    else:
        datetime_end = datetime.fromisoformat(datetime_end)

    return datetime_start, datetime_end


@task(cache_policy=NO_CACHE)
def get_journeys(project_name: str, datetime_start: datetime, datetime_end: datetime) -> Path:
    datetime_start_str = datetime_start.isoformat()
    datetime_end_str = datetime_end.isoformat()
    query = f"""
        SELECT
            j.journey_key,
            j.stop_id_1,
            j.stop_name_1,
            j.stop_id_2,
            j.stop_name_2,
            j.stop_id_3,
            j.stop_name_3,
            j.stop_id_4,
            j.stop_name_4
        FROM {constants.JOURNEY_TABLE_NAME} j
        JOIN rj-smtr.cadastro_interno.cliente_jae c ON j.rider_id = sha256(c.id_cliente)
        WHERE
            j.service_date BETWEEN date('{datetime_start_str}') AND date('{datetime_end_str}')
            AND datetime(j.service_date, j.alighting_time)
                BETWEEN datetime('{datetime_start_str}')
                AND datetime('{datetime_end_str}')
            AND c.documento IN UNNEST(@volunteers)
    """

    save_filepath = constants.JOUNEYS_DATA_FILEPATH

    pandas_gbq.read_gbq(
        query,
        project_id=project_name,
        configuration={
            "query": {
                "parameterMode": "NAMED",
                "queryParameters": [
                    {
                        "name": "volunteers",
                        "parameterType": {"type": "ARRAY", "arrayType": {"type": "STRING"}},
                        "parameterValue": {"arrayValues": [{"value": x} for x in get_volunteers()]},
                    }
                ],
            }
        },
    ).to_csv(str(save_filepath), header=True, index=False)

    return save_filepath


@task(cache_policy=NO_CACHE)
def generate_images(bucket_obj: storage.Bucket, journey_filepath: Path) -> list[Path]:
    df_jouney = pd.read_csv(str(journey_filepath), chunksize=2000)
    font = load_font("DejaVuSans-Bold.ttf", 80)
    caption_font = load_font("DejaVuSans.ttf", 18)
    downloaded_images = []

    result_paths = []
    for chunk in df_jouney:
        for _, row in chunk.iterrows():
            stop_info = {row[f"stop_id_{i}"]: row[f"stop_name_{i}"] for i in range(1, 5)}
            result_paths.append(
                build_journey_image(
                    bucket_obj=bucket_obj,
                    journey_key=row["journey_key"],
                    stop_info=stop_info,
                    font=font,
                    caption_font=caption_font,
                    downloaded_images=downloaded_images,
                )
            )
    return result_paths


@task(cache_policy=NO_CACHE)
def upload_images_to_gcs(bucket_obj: storage.Bucket, filepaths: list[Path]):
    for path in filepaths:
        upload_image_to_gcs(bucket_obj=bucket_obj, image_local_path=path)


@task(cache_policy=NO_CACHE)
def save_run_on_redis(
    env: str,
    datetime_start: datetime,
    datetime_end: datetime,
):
    redis_key = f"{env}.{constants.REDIS_KEY_SUFIX}"
    redis_client = get_redis_client()
    result = redis_client.get(redis_key)

    if result is None:
        result = {}
        last_run = {constants.REDIS_DICT_KEY: constants.FIRST_TIMESTAMP}
    else:
        last_run = last_run[constants.REDIS_DICT_KEY]

    last_run = datetime.fromisoformat(
        redis_client.get(redis_key) or {constants.REDIS_DICT_KEY: constants.FIRST_TIMESTAMP}
    )

    if last_run < datetime_end and last_run >= datetime_start:
        result[constants.REDIS_DICT_KEY] = datetime_end.isoformat()
        print(f"Salvando valor: {result} na key: {redis_key}")
        redis_client.set(redis_key, result)
        print("Valor salvo no Redis!")
