# -*- coding: utf-8 -*-
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import OperationalError

ENGINE_MAPPING = {
    "mysql": {"driver": "pymysql", "port": "3306"},
    "postgresql": {"driver": "psycopg2", "port": "5432"},
}


def create_database_url(
    engine: str,
    host: str,
    user: str,
    password: str,
    database: str,
) -> str:
    """
    Cria a URL para se conectar a um banco de dados

    Args:
        engine (str): O banco de dados (postgresql ou mysql)
        host (str): O host do banco de dados
        user (str): O usuário para se conectar
        password (str): A senha do usuário
        database (str): O nome da base (schema)

    Returns:
        str: a URL de conexão
    """
    engine_info = ENGINE_MAPPING[engine]
    driver = engine_info["driver"]
    port = engine_info["port"]
    return f"{engine}+{driver}://{user}:{password}@{host}:{port}/{database}"


def test_database_connection(
    engine: str,
    host: str,
    user: str,
    password: str,
    database: str,
) -> tuple[bool, str]:
    """
    Testa se é possível se conectar a um banco de dados

    Args:
        engine (str): O banco de dados (postgresql ou mysql)
        host (str): O host do banco de dados
        user (str): O usuário para se conectar
        password (str): A senha do usuário
        database (str): O nome da base (schema)

    Returns:
        bool: Se foi possível se conectar ou não
        str: String do erro
    """
    url = create_database_url(
        engine=engine,
        host=host,
        user=user,
        password=password,
        database=database,
    )
    connection = create_engine(url)
    print(f"Tentando conexão com o banco de dados {database}")
    try:
        with connection.connect() as _:
            print("Conexão bem-sucedida!")
            return True, None
    except OperationalError as e:
        print("Conexão falhou", level="warning")
        return False, str(e)


def list_accessible_tables(
    engine: Engine,
) -> list[str]:
    """
    Retorna a lista de tabelas acessíveis para o usuário no banco de dados conectado.

    Args:
        engine (Engine): Objeto SQLAlchemy Engine representando a conexão com o banco de dados.

    Returns:
        list[str]: Lista com os nomes das tabelas acessíveis.
    """
    db_type = engine.dialect.name

    if db_type == "postgresql":
        query = text(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            and table_type = 'BASE TABLE'
        """
        )
    elif db_type == "mysql":
        query = text(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = DATABASE()
            and table_type = 'BASE TABLE'
        """
        )
    else:
        raise ValueError(f"Banco de dados {db_type} não suportado")

    with engine.connect() as conn:
        result = conn.execute(query)
        return [row[0] for row in result]
