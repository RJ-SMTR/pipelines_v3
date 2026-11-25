# -*- coding: utf-8 -*-
from datetime import datetime
from pathlib import Path
from time import sleep

import pandas as pd
import pandas_gbq
from google.api_core.exceptions import NotFound
from google.cloud import storage
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common.tasks import get_scheduled_timestamp
from pipelines.common.treatment.default_treatment.utils import IncompleteDataError
from pipelines.common.utils.redis import get_redis_client
from pipelines.common.utils.utils import convert_timezone
from pipelines.integration__imagem_pesquisa_matriz_od import constants
from pipelines.integration__imagem_pesquisa_matriz_od.utils import (
    build_journey_image,
    load_font,
    upload_image_to_gcs,
)
from pipelines.treatment__mit_ipea_project import constants as materialization_constants


@task(cache_policy=NO_CACHE)
def get_date_range(
    env: str,
    datetime_start: str | None,
    datetime_end: str | None,
) -> tuple[datetime, datetime]:
    """
    Retorna as datas de inicio e fim para a execução do flow com base nos parâmetros fornecidos.

    Args:
        env (str): prod ou dev.
        datetime_start (Optional[str]): Parâmetro do flow de datetime início.
        datetime_end (Optional[str]): Parâmetro do flow de datetime fim.
    Returns:
        datetime: datetime de início.
        datetime: datetime de fim.
    """
    if datetime_start is None:
        redis_client = get_redis_client()
        last_run = redis_client.get(f"{env}.{constants.REDIS_KEY_SUFIX}")
        if last_run is None:
            last_run = {constants.REDIS_DICT_KEY: constants.FIRST_TIMESTAMP}
        else:
            last_run = last_run[constants.REDIS_DICT_KEY]

    datetime_start = convert_timezone(
        datetime.fromisoformat(last_run.get(constants.REDIS_DICT_KEY))
    )

    if datetime_end is None:
        datetime_end = get_scheduled_timestamp()
    else:
        datetime_end = datetime.fromisoformat(datetime_end)

    print(f"datetime_start: {datetime_start}")
    print(f"datetime_end: {datetime_end}")

    return datetime_start, datetime_end


@task(cache_policy=NO_CACHE)
def wait_materialization(env: str, datetime_end: datetime):
    count = 0
    wait_limit = 10
    complete = False
    print("Checando completude dos dados")
    while not complete:
        complete = materialization_constants.MIT_IPEA_PROJECT_SELECTOR.is_up_to_date(
            env=env,
            timestamp=datetime_end,
        )
        if not complete:
            if count < wait_limit:
                print("Dados incompletos, tentando novamente")
                sleep(60)
                count += 1
            else:
                print("Tempo de espera esgotado!")
                raise IncompleteDataError("Dados incompletos!")
    print("Dados completos!")


@task(cache_policy=NO_CACHE)
def get_journeys(
    project_name: str,
    datetime_start: datetime,
    datetime_end: datetime,
) -> Path:
    """
    Consulta a tabela de matriz OD do BigQuery e salva os dados como CSV.

    Args:
        project_name (str): Nome do projeto GCP.
        datetime_start (datetime): Datetime de início.
        datetime_end (datetime): Datetime de fim.

    Returns:
        Path: Caminho do CSV com os dados da consulta
    """
    datetime_start_str = datetime_start.strftime("%Y-%m-%d %H:%M:%S")
    datetime_end_str = datetime_end.strftime("%Y-%m-%d %H:%M:%S")
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
            AND j.datetime_ultima_atualizacao
                BETWEEN datetime('{datetime_start_str}')
                AND datetime_add(datetime('{datetime_end_str}'), interval 1 hour)
    """

    save_filepath = constants.JOUNEYS_DATA_FILEPATH

    save_filepath.parent.mkdir(parents=True, exist_ok=True)

    pandas_gbq.read_gbq(
        query,
        project_id=project_name,
    ).to_csv(str(save_filepath), header=True, index=False)

    return save_filepath


@task(cache_policy=NO_CACHE)
def generate_images(bucket_obj: storage.Bucket, journey_filepath: Path) -> list[Path]:
    """
    Gera as imagens com o grid com as 4 opções de desembarque.

    Args:
        bucket_obj (storage.Bucket): Objeto de Bucket GCS.
        journey_filepath: (Path): Caminho do CSV com os dados das viagens.

    Returns:
        list[Path]: Lista com os caminhos das imagens.
    """
    df_jouney = pd.read_csv(str(journey_filepath), chunksize=2000)
    font = load_font("DejaVuSans-Bold.ttf", 80)
    caption_font = load_font("DejaVuSans.ttf", 18)
    downloaded_images = []

    result_paths = []
    for chunk in df_jouney:
        for _, row in chunk.iterrows():
            stop_info = {row[f"stop_id_{i}"]: row[f"stop_name_{i}"] for i in range(1, 5)}
            try:
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
            except NotFound:
                continue
    return result_paths


@task(cache_policy=NO_CACHE)
def upload_images_to_gcs(bucket_obj: storage.Bucket, filepaths: list[Path]):
    """
    Faz upload das imagens para o GCS

    Args:
        bucket_obj (storage.Bucket): Objeto de Bucket GCS.
        filepaths (list[Path]): Lista com os caminhos das imagens.
    """
    for path in filepaths:
        upload_image_to_gcs(bucket_obj=bucket_obj, image_local_path=path)


@task(cache_policy=NO_CACHE)
def save_run_on_redis(
    env: str,
    datetime_start: datetime,
    datetime_end: datetime,
):
    """
    env (str): prod ou dev.
    datetime_start (datetime): Datetime de início da execução.
    datetime_end (datetime): Datetime de fim da execução.
    """
    redis_key = f"{env}.{constants.REDIS_KEY_SUFIX}"
    redis_client = get_redis_client()
    result = redis_client.get(redis_key)

    if result is None:
        result = {}
        last_run = constants.FIRST_TIMESTAMP
    else:
        last_run = last_run[constants.REDIS_DICT_KEY]

    last_run = convert_timezone(datetime.fromisoformat(last_run))

    print(f"Valor atual: {last_run}")

    if last_run < datetime_end and last_run >= datetime_start:
        result[constants.REDIS_DICT_KEY] = datetime_end.isoformat()
        print(f"Salvando valor: {result} na key: {redis_key}")
        redis_client.set(redis_key, result)
        print("Valor salvo no Redis!")
