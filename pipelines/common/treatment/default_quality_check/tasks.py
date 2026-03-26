# -*- coding: utf-8 -*-
from datetime import datetime
from typing import Optional

from prefect import task
from prefect.cache_policies import NO_CACHE

from pipelines.common.treatment.default_quality_check.utils import run_dbt_tests
from pipelines.common.treatment.default_treatment.utils import (
    DBTTest,
    dbt_test_notify_discord,
)
from pipelines.common.utils.redis import get_redis_client
from pipelines.common.utils.utils import convert_timezone


@task(cache_policy=NO_CACHE)
def get_quality_check_datetime_start(
    env: str,
    dbt_test: DBTTest,
    datetime_start: Optional[str],
    partitions: Optional[list],
) -> Optional[datetime]:
    """
    Consulta o Redis para pegar o datetime inicial para fazer o teste
    de qualidade dos dados

    Args:
        env (str): prod ou dev.
        dbt_test (DBTTest): Objeto representando o teste do DBT.
        datetime_start (Optional[str]): Parâmetro do flow para definir o valor manualmente.
        partitions (Optional[list[str]]): Lista de partições para execução dos testes.

    returns:
        Optional[datetime]: datetime salvo no redis, valor do
        parâmetro datetime_start ou None caso os dois anteriores sejam nulos
        ou se o parâmetro partitions não for nulo.
    """
    if dbt_test.test_alias is None:
        raise AttributeError("O atributo test_alias do DBTTest não pode ser nulo!")
    if partitions is not None:
        return
    if datetime_start is not None:
        return convert_timezone(datetime.fromisoformat(datetime_start))

    redis_key = f"{env}.{dbt_test.test_alias}.data_quality_check"
    client = get_redis_client()
    print(f"Consultando Redis. key = {redis_key}")
    content = client.get(redis_key)
    if content is None:
        return content
    print(f"content = {content}")
    return convert_timezone(datetime.fromisoformat(content["last_run_timestamp"]))


@task(cache_policy=NO_CACHE)
def get_quality_check_datetime_end(
    timestamp: datetime,
    datetime_start: Optional[datetime],
    datetime_end: Optional[str],
    partitions: Optional[list],
) -> Optional[datetime]:
    """
    Retorna o datetime final para fazer o teste de qualidade dos dados

    Args:
        timestamp (datetime): Datetime do agendamento da run atual do Flow.
        datetime_start (Optional[datetime]): Datetime inicial da execução.
        datetime_end (Optional[str]): Parâmetro do flow para definir o valor manualmente.
        partitions (Optional[list[str]]): Lista de partições para execução dos testes.

    returns:
        Optional[datetime]: datetime agendado da execução do flow, valor do
        parâmetro datetime_end ou None se o parâmetro partitions não for nulo.
    """
    if partitions is not None:
        return
    if datetime_end is not None:
        datetime_end = convert_timezone(datetime.fromisoformat(datetime_end))
    else:
        datetime_end = timestamp

    if datetime_start is not None and datetime_end < datetime_start:
        raise ValueError("datetime de início maior que datetime de fim!")

    print(f"datetime_end = {datetime_end}")
    return datetime_end


@task(cache_policy=NO_CACHE)
def task_run_dbt_tests(
    dbt_test: DBTTest,
    datetime_start: Optional[datetime],
    datetime_end: Optional[datetime],
    partitions: Optional[list[str]],
) -> tuple[str, dict]:
    """
    Executa o DBT test

    Args:
        dbt_test (DBTTest): Objeto representando o teste do DBT.
        datetime_start (Optional[datetime]): Datetime inicial da execução.
        datetime_end (Optional[datetime]): Datetime final da execução.
        partitions (Optional[list[str]]): Lista de partições para execução dos testes.
    """
    return run_dbt_tests(
        dbt_test=dbt_test,
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        partitions=partitions,
    )


@task(cache_policy=NO_CACHE)
def task_dbt_test_notify_discord(  # noqa: PLR0913
    dbt_test: DBTTest,
    dbt_vars: dict,
    dbt_logs: str,
    webhook_key: str = "dataplex",
    raise_check_error: bool = True,
    additional_mentions: Optional[list] = None,
):
    """
    Processa os resultados dos testes do dbt e envia notificações para o Discord.

    Args:
        dbt_test (DBTTest): Objeto que representa o teste do dbt.
        dbt_vars(dict): Dicionário contendo as variáveis utilizadas na execução do teste.
        dbt_logs (str): Logs retornados pelo DBT.
        webhook_key (str): Chave do webhook do Discord.
        raise_check_error (bool): Indica se deve lançar erro em caso de falha nos testes.
        additional_mentions (Optional[list]): Menções adicionais na mensagem.
    """
    dbt_test_notify_discord(
        dbt_test=dbt_test,
        dbt_vars=dbt_vars,
        dbt_logs=dbt_logs,
        webhook_key=webhook_key,
        raise_check_error=raise_check_error,
        additional_mentions=additional_mentions,
    )


@task(cache_policy=NO_CACHE)
def set_redis_quality_check_datetime(
    env: str,
    dbt_test: DBTTest,
    datetime_end: Optional[datetime],
):
    """
    Define o valor final da execução no Redis

    Args:
        env (str): prod ou dev.
        dbt_test (DBTTest): Objeto que representa o teste do dbt.
        datetime_end (Optional[datetime]): Datetime final da execução
    """
    if dbt_test.test_alias is None:
        raise AttributeError("O atributo test_alias do DBTTest não pode ser nulo!")
    if datetime_end is None:
        return
    value = datetime_end.isoformat()
    redis_key = f"{env}.{dbt_test.test_alias}.data_quality_check"
    print(f"salvando timestamp {value} na key: {redis_key}")
    redis_client = get_redis_client()
    content = redis_client.get(redis_key)
    if not content:
        content = {"last_run_timestamp": value}
        redis_client.set(redis_key, content)
    elif (
        convert_timezone(
            datetime.fromisoformat(
                content["last_run_timestamp"],
            )
        )
        < datetime_end
    ):
        content["last_run_timestamp"] = value
        redis_client.set(redis_key, content)
