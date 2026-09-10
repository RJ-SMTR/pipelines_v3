# -*- coding: utf-8 -*-
"""Tasks de captura dos dados de temperatura do INMET"""

from functools import partial
from time import sleep

import requests
from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.capture__inmet_temperatura import constants
from pipelines.common.capture.default_capture.utils import SourceCaptureContext
from pipelines.common.utils.fs import save_local_file
from pipelines.common.utils.secret import get_env_secret

MAX_ATTEMPTS = 3
RETRY_DELAY_SECONDS = 30
REQUEST_TIMEOUT_SECONDS = 60
HTTP_SERVER_ERROR = 500
HTTP_TOO_MANY_REQUESTS = 429


def get_inmet_station_data(capture_date: str, station: str, key: str) -> list[dict]:
    """Consulta uma estação com diagnóstico seguro e tentativas limitadas."""
    url = f"{constants.INMET_BASE_URL}/{capture_date}/{capture_date}/{station}/{key}"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        retryable = True
        try:
            with requests.get(url, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                content_type = response.headers.get("Content-Type", "").replace(key, "[REDACTED]")
                print(
                    f"INMET data={capture_date} estação={station} "
                    f"tentativa={attempt}/{MAX_ATTEMPTS} "
                    f"status={response.status_code} content_type={content_type!r} "
                    f"bytes={len(response.content)}"
                )
                if response.ok:
                    try:
                        data = response.json()
                    except ValueError:
                        reason = "resposta sem JSON válido"
                    else:
                        if isinstance(data, list) and all(isinstance(row, dict) for row in data):
                            return data
                        reason = "JSON fora do formato esperado (lista de registros)"
                else:
                    reason = f"HTTP {response.status_code}"
                    retryable = (
                        response.status_code >= HTTP_SERVER_ERROR
                        or response.status_code == HTTP_TOO_MANY_REQUESTS
                    )
        except requests.RequestException as error:
            reason = type(error).__name__

        print(f"INMET data={capture_date} estação={station}: {reason}")
        if not retryable or attempt == MAX_ATTEMPTS:
            break
        sleep(RETRY_DELAY_SECONDS * attempt)

    raise RuntimeError(
        f"Falha INMET data={capture_date} estação={station} após {attempt} tentativa(s): {reason}"
    ) from None


def extract_inmet_data(capture_date: str, raw_filepath: str, key: str) -> list[str]:
    """Salva os dados somente após consultar todas as estações com sucesso."""
    data = []
    for station in constants.INMET_ESTACOES:
        data.extend(get_inmet_station_data(capture_date, station, key))
    filepath = raw_filepath.format(page=0)
    save_local_file(filepath=filepath, filetype="json", data=data)
    return [filepath]


@task(cache_policy=NO_CACHE)
def create_temperatura_extractor(context: SourceCaptureContext):
    """Cria a extração de dados da api do INMET"""

    capture_date = context.timestamp.strftime("%Y-%m-%d")

    key = get_env_secret(constants.INMET_SECRET_PATH)["key"]

    return partial(
        extract_inmet_data, capture_date=capture_date, raw_filepath=context.raw_filepath, key=key
    )
