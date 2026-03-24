# -*- coding: utf-8 -*-
from prefect import task

from pipelines.common import constants as smtr_constants
from pipelines.common.capture.jae import constants
from pipelines.common.utils.database import test_database_connection
from pipelines.common.utils.secret import get_env_secret


@task
def test_jae_databases_connections() -> tuple[bool, list[str]]:
    """
    Testa a conexão com os bancos de dados da Jaé

    Returns:
        bool: Se todas as conexões foram bem-sucedidas ou não
        list[str]: Lista com os nomes dos bancos de dados com falha de conexão
    """
    credentials = get_env_secret(constants.JAE_SECRET_PATH)
    failed_connections = []
    for database_name, database in constants.JAE_DATABASE_SETTINGS.items():
        success, _ = test_database_connection(
            engine=database["engine"],
            host=database["host"],
            user=credentials["user"],
            password=credentials["password"],
            database=database_name,
        )
        if not success:
            failed_connections.append(database_name)

    return len(failed_connections) == 0, failed_connections


@task
def create_database_error_discord_message(failed_connections: list[str]) -> str:
    """
    Cria a mensagem para ser enviada no Discord caso haja
    problemas de conexão com os bancos da Jaé

    Args:
        failed_connections (list[str]): Lista com os nomes dos bancos de dados com falha de conexão
    Returns:
        str: Mensagem
    """
    message = "Falha de conexão com o(s) banco(s) de dados:\n"
    failed_connections = "\n".join(failed_connections)
    message += failed_connections
    return (
        message
        + "\n"
        + f" - <@&{smtr_constants.OWNERS_DISCORD_MENTIONS['dados_smtr']['user_id']}>\n"
    )
