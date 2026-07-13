# -*- coding: utf-8 -*-
import inspect
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections import defaultdict
from datetime import datetime, time, timedelta
from functools import cached_property
from pathlib import Path
from typing import Optional, Union
from zoneinfo import ZoneInfo

import pytz
import requests
import yaml
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from prefect import runtime
from prefect_dbt import PrefectDbtRunner, PrefectDbtSettings

from pipelines.common import constants as smtr_constants
from pipelines.common.treatment.default_treatment import constants
from pipelines.common.utils.cron import cron_get_last_date, cron_get_next_date
from pipelines.common.utils.deployment import get_flow_folder_path, get_flow_schedule_cron
from pipelines.common.utils.discord import format_send_discord_message
from pipelines.common.utils.fs import get_project_root_path
from pipelines.common.utils.gcp.bigquery import SourceTable
from pipelines.common.utils.prefect import rename_flow_run
from pipelines.common.utils.redis import get_redis_client
from pipelines.common.utils.secret import get_env_secret
from pipelines.common.utils.utils import convert_timezone, is_running_locally


class DBTTest:
    """
    Representa a configuração de um teste do DBT a ser executado em uma materialização.

    Args:
        test_select (str): Select do dbt que define quais testes serão executados.
        exclude (Optional[str]): Parâmetro exclude do dbt.
        test_descriptions (Optional[dict]): Descrições dos testes para uso em notificações.
        delay_days_start (int): Dias subtraídos do datetime inicial da materialização.
        delay_days_end (int): Dias subtraídos do datetime final da materialização.
        truncate_date (bool): Se True, ajusta o intervalo para o dia inteiro.
        additional_vars (Optional[dict]): Variáveis adicionais para o dbt.
        test_alias (Optional[str]): Define um alias para o teste.
    """

    def __init__(  # noqa: PLR0913
        self,
        test_select: str,
        exclude: Optional[str] = None,
        test_descriptions: Optional[dict] = None,
        delay_days_start: int = 0,
        delay_days_end: int = 0,
        truncate_date: bool = False,
        additional_vars: Optional[dict] = None,
        test_start_datetime: Optional[datetime] = None,
        test_alias: Optional[str] = None,
    ):
        self.test_select = test_select
        self.exclude = exclude
        self.test_descriptions = test_descriptions or {}
        self.delay_days_start = delay_days_start
        self.delay_days_end = delay_days_end
        self.truncate_date = truncate_date
        self.additional_vars = additional_vars or {}
        self.test_start_datetime = test_start_datetime
        self.test_alias = test_alias

    def __getitem__(self, key):
        return self.__dict__[key]

    def get_test_vars(
        self,
        datetime_start: datetime,
        datetime_end: datetime,
        partitions: Optional[list[str]] = None,
    ) -> dict:
        """
        Gera as variáveis para execução do teste do dbt.

        Args:
            datetime_start (datetime): Datetime inicial da materialização.
            datetime_end (datetime): Datetime final da materialização.

        Returns:
            dict: Variáveis formatadas para execução do teste do dbt.
        """

        pattern = constants.MATERIALIZATION_LAST_RUN_PATTERN

        datetime_start, datetime_end = self.adjust_datetime_range(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
        )

        final_dict = {
            "date_range_start": datetime_start.strftime(pattern),
            "date_range_end": datetime_end.strftime(pattern),
        }

        if partitions is not None:
            final_dict["partitions"] = partitions

        collision = final_dict.keys() & self.additional_vars.keys()
        if collision:
            raise ValueError(f"Variáveis reservadas não podem ser sobrescritas: {collision}")

        return final_dict | self.additional_vars

    def adjust_datetime_range(
        self, datetime_start: datetime, datetime_end: datetime
    ) -> tuple[datetime, datetime]:
        """
        Ajusta o range de datetime

        Args:
            datetime_start (datetime): Datetime inicial
            datetime_end (datetime): Datetime final

        Returns:
            tuple[datetime, datetime]: (datetime_start, datetime_end) ajustados
        """

        adjusted_start = datetime_start
        adjusted_end = datetime_end

        adjusted_start = adjusted_start - timedelta(days=self.delay_days_start)
        adjusted_end = adjusted_end - timedelta(days=self.delay_days_end)

        if self.truncate_date:
            adjusted_start = adjusted_start.replace(hour=0, minute=0, second=0, microsecond=0)
            adjusted_end = adjusted_end.replace(hour=23, minute=59, second=59, microsecond=0)

        return adjusted_start, adjusted_end


class DBTSelector:
    """
    Representa um selector do DBT com controle de agendamento e estado de materialização.

    Args:
        name (str): Nome do selector no DBT.
        initial_datetime (datetime): Datetime inicial permitido para materialização.
        final_datetime (Optional[datetime]): Datetime final permitido para materialização.
        flow_folder_name (Optional[str]): Nome da pasta do flow no Prefect.
        incremental_delay_hours (int): Horas subtraídas do datetime final.
        redis_key_suffix (Optional[str]): Sufixo para a chave do Redis.
        pre_test (Optional[DBTTest]): Teste executado antes da materialização.
        post_test (Optional[DBTTest]): Teste executado após a materialização.
        data_sources (Optional[list[Union["DBTSelector", SourceTable, dict]]]): Fontes de dados
            associadas ao selector.
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        initial_datetime: datetime,
        final_datetime: Optional[datetime] = None,
        flow_folder_name: Optional[str] = None,
        incremental_delay_hours: int = 0,
        redis_key_suffix: Optional[str] = None,
        pre_test: Optional[DBTTest] = None,
        post_test: Optional[DBTTest] = None,
        data_sources: Optional[list[Union["DBTSelector", SourceTable, dict]]] = None,
    ):
        self.name = name
        self.flow_folder_name = flow_folder_name
        self.incremental_delay_hours = incremental_delay_hours
        self.initial_datetime = convert_timezone(initial_datetime)
        self.final_datetime = (
            final_datetime if final_datetime is None else convert_timezone(final_datetime)
        )
        self.redis_key_suffix = redis_key_suffix
        self.pre_test = pre_test
        self.post_test = post_test

        self.data_sources = data_sources or []

        if flow_folder_name is None:
            self.flow_folder_path = Path(inspect.stack()[1].filename).parent
        else:
            self.flow_folder_path = get_flow_folder_path(flow_folder_name)

    def __getitem__(self, key):
        return getattr(self, key)

    def _get_redis_key(self, env: str) -> str:
        """
        Gera a chave do Redis para o selector

        Args:
            env (str): prod ou dev

        Returns:
            str: chave do Redis
        """
        redis_key = f"{env}.selector_{self.name}"
        if self.redis_key_suffix:
            return f"{redis_key}_{self.redis_key_suffix}"
        return redis_key

    @cached_property
    def schedule_cron(self) -> Optional[str]:
        """
        Cron do schedule do deployment do flow associado ao Selector. Lido do prefect.yaml
        no primeiro acesso.
        """
        return get_flow_schedule_cron(self.flow_folder_path)

    def get_last_materialized_datetime(self, env: str) -> Optional[datetime]:
        """
        Pega o último datetime materializado no Redis

        Args:
            env (str): prod ou dev

        Returns:
            datetime: a data vinda do Redis
        """
        redis_key = self._get_redis_key(env)
        redis_client = get_redis_client()
        content = redis_client.get(redis_key)
        if content is None:
            last_datetime = self.initial_datetime
        else:
            last_datetime = datetime.strptime(
                content[constants.REDIS_LAST_MATERIALIZATION_TS_KEY],
                constants.MATERIALIZATION_LAST_RUN_PATTERN,
            ).replace(tzinfo=ZoneInfo(smtr_constants.TIMEZONE))

        return convert_timezone(timestamp=last_datetime)

    def get_datetime_end(self, timestamp: datetime) -> datetime:
        """
        Calcula o datetime final da materialização com base em um timestamp

        Args:
            timestamp (datetime): datetime de referência

        Returns:
            datetime: datetime_end calculado
        """
        return timestamp - timedelta(hours=self.incremental_delay_hours)

    def is_up_to_date(self, env: str, timestamp: datetime) -> bool:
        """
        Confere se o selector está atualizado em relação a um timestamp

        Args:
            env (str): prod ou dev
            timestamp (datetime): datetime de referência

        Returns:
            bool: se está atualizado ou não
        """
        if self.schedule_cron is None:
            raise ValueError("O selector não possui agendamento")
        last_materialization = self.get_last_materialized_datetime(env=env)

        last_schedule = cron_get_last_date(cron_expr=self.schedule_cron, timestamp=timestamp)

        if self.final_datetime is not None:
            last_schedule = min(last_schedule, self.final_datetime)

        return last_materialization >= last_schedule - timedelta(hours=self.incremental_delay_hours)

    def get_next_schedule_datetime(self, timestamp: datetime) -> datetime:
        """
        Pega a próxima data de execução do selector em relação a um datetime
        com base no schedule_cron

        Args:
            timestamp (datetime): datetime de referência

        Returns:
            datetime: próximo datetime do cron
        """
        if self.schedule_cron is None:
            raise ValueError("O selector não possui agendamento")
        return cron_get_next_date(cron_expr=self.schedule_cron, timestamp=timestamp)

    def set_redis_materialized_datetime(self, env: str, timestamp: datetime):
        """
        Atualiza a timestamp de materialização no Redis

        Args:
            env (str): prod ou dev
            timestamp (datetime): data a ser salva no Redis
        """
        value = timestamp.strftime(constants.MATERIALIZATION_LAST_RUN_PATTERN)
        redis_key = self._get_redis_key(env)
        print(f"Salvando timestamp {value} na key: {redis_key}")
        redis_client = get_redis_client()
        content = redis_client.get(redis_key)
        if not content:
            content = {constants.REDIS_LAST_MATERIALIZATION_TS_KEY: value}
            redis_client.set(redis_key, content)
        elif (
            convert_timezone(
                datetime.strptime(
                    content[constants.REDIS_LAST_MATERIALIZATION_TS_KEY],
                    constants.MATERIALIZATION_LAST_RUN_PATTERN,
                ).replace(tzinfo=ZoneInfo(smtr_constants.TIMEZONE))
            )
            < timestamp
        ):
            content[constants.REDIS_LAST_MATERIALIZATION_TS_KEY] = value
            redis_client.set(redis_key, content)


def get_repo_version() -> str:
    """
    Retorna o SHA do último commit do repositório no GitHub.

    Returns:
        str: SHA do último commit do repositório.
    """
    response = requests.get(
        f"{constants.REPO_URL}/commits",
        timeout=60,
    )
    response.raise_for_status()
    return response.json()[0]["sha"]


class DBTSelectorMaterializationContext:
    def __init__(  # noqa: PLR0913
        self,
        env: str,
        selector: DBTSelector,
        timestamp: datetime,
        datetime_start: Optional[str],
        datetime_end: Optional[str],
        additional_vars: Optional[dict],
        test_scheduled_time: Optional[time],
        force_test_run: bool,
        snapshot_selector: Optional[DBTSelector] = None,
        skip_pre_test: bool = False,
    ):
        """
        Armazena o contexto completo necessário para materializar um selector do DBT.

        Args:
            env (str): prod ou dev
            selector (DBTSelector): Selector associado à materialização.
            timestamp (datetime): Timestamp de execução do fluxo.
            datetime_start (Optional[str]): Datetime inicial forçado.
            datetime_end (Optional[str]): Datetime final forçado.
            additional_vars (Optional[dict]): Variáveis adicionais do dbt.
            test_scheduled_time (Optional[time]): Horário agendado para execução dos testes.
            force_test_run (bool): Força a execução dos testes.
            snapshot_selector (Optional[DBTSelector]): Selector para snapshot opcional.
            skip_pre_test (bool): Se True, ignora a execução do pre_test do selector.
        """
        self.env = env
        self.selector = selector
        self.snapshot_selector = snapshot_selector
        self.timestamp = timestamp.astimezone(tz=pytz.timezone(smtr_constants.TIMEZONE))
        self.datetime_start = self.get_datetime_start(datetime_start=datetime_start)
        self.datetime_end = self.get_datetime_end(datetime_end=datetime_end)

        self.dbt_vars = self.get_dbt_vars(
            datetime_start=self.datetime_start,
            datetime_end=self.datetime_end,
            additional_vars=additional_vars,
        )

        self.should_run = (
            False
            if (
                selector.final_datetime is not None
                and self.datetime_start >= selector.final_datetime
            )
            else True
        )

        is_test_scheduled_time = (
            force_test_run or test_scheduled_time is None or timestamp.time() == test_scheduled_time
        ) and self.should_run

        self.should_run_pre_test = (
            selector.pre_test is not None and is_test_scheduled_time and not skip_pre_test
        )

        self.should_run_post_test = selector.post_test is not None and is_test_scheduled_time

        self.pre_test_dbt_vars = (
            selector.pre_test.get_test_vars(
                datetime_start=self.datetime_start,
                datetime_end=self.datetime_end,
            )
            if self.should_run_pre_test
            else None
        )

        self.post_test_dbt_vars = (
            selector.post_test.get_test_vars(
                datetime_start=self.datetime_start,
                datetime_end=self.datetime_end,
            )
            if self.should_run_post_test
            else None
        )

        self.pre_test_log = None
        self.post_test_log = None

    def __getitem__(self, key):
        return self.__dict__[key]

    def __setitem__(self, key, value):
        self.__dict__[key] = value

    def get_datetime_start(
        self,
        datetime_start: Optional[str],
    ) -> Optional[datetime]:
        """
        Retorna o datetime de inicio da materialização

        Args:
            datetime_start (Optional[str]): Força um valor no datetime_start

        Returns:
            Optional[datetime]: datetime de inicio da materialização
        """
        if datetime_start is not None:
            datetime_start = datetime.fromisoformat(datetime_start)
        else:
            datetime_start = self.selector.get_last_materialized_datetime(env=self.env)

        datetime_start = convert_timezone(timestamp=datetime_start)

        if datetime_start < self.selector.initial_datetime:
            return self.selector.initial_datetime

        return datetime_start

    def get_datetime_end(
        self,
        datetime_end: Optional[str],
    ) -> datetime:
        """
        Retorna o datetime de fim da materialização

        Args:
            datetime_end (Optional[str]): Força um valor no datetime_end

        Returns:
            datetime: datetime de fim da materialização
        """
        if datetime_end is not None:
            datetime_end = datetime.fromisoformat(datetime_end)
        else:
            datetime_end = self.selector.get_datetime_end(timestamp=self.timestamp)

        datetime_end = convert_timezone(timestamp=datetime_end)

        if self.selector.final_datetime is not None and datetime_end > self.selector.final_datetime:
            return self.selector.final_datetime

        return datetime_end

    def get_dbt_vars(
        self,
        datetime_start: datetime,
        datetime_end: datetime,
        additional_vars: Optional[dict],
    ):
        """
        Cria a lista de variaveis para rodar o modelo DBT,
        unindo a versão do repositório com as variaveis de datetime

        Args:
            datetime_start (datetime): Datetime inicial da materialização parametrizado
            datetime_end (datetime): Datetime final da materialização parametrizado
            additional_vars (dict): Variáveis extras para executar o modelo DBT

        Returns:
            dict[str]: Variáveis para executar o modelo DBT
        """

        pattern = constants.MATERIALIZATION_LAST_RUN_PATTERN

        dbt_vars = {
            "date_range_start": datetime_start.strftime(pattern),
            "date_range_end": datetime_end.strftime(pattern),
            "version": get_repo_version(),
            "start_date": datetime_start.strftime("%Y-%m-%d"),
            "end_date": datetime_end.strftime("%Y-%m-%d"),
        }

        if additional_vars:
            dbt_vars.update(additional_vars)

        return dbt_vars


def get_dbt_target(env: str) -> str:
    """
    Retorna o target do dbt com base no ambiente e no contexto de execução.

    Args:
        env (str): Ambiente de execução ("prod" ou "dev").

    Returns:
        str: "prod" se env for prod; "dev" se rodando localmente; "hmg" caso contrário.
    """
    if env == "prod":
        return "prod"
    return "dev" if is_running_locally() else "hmg"


def get_dbt_paths() -> tuple[Path, Path, Path]:
    """
    Resolve os diretórios padrão do projeto dbt.

    Returns:
        tuple[Path, Path, Path]: ``project_dir`` (pasta ``queries``), ``profiles_dir``
        (``queries/dev`` localmente, ``queries`` em deploy) e ``target_path``
        (``queries/target``).
    """
    root_path = get_project_root_path()
    project_dir = root_path / "queries"
    profiles_dir = project_dir / "dev" if is_running_locally() else project_dir
    target_path = project_dir / "target"
    return project_dir, profiles_dir, target_path


def get_dbt_selection_args(
    dbt_obj: Optional[DBTSelector],
    dbt_command: Optional[Union[str, list[str]]],
) -> list[str]:
    """
    Extrai os argumentos de seleção dbt (``--select``/``--exclude``/``--selector``).

    Reutiliza a seleção da materialização normal para reaproveitá-la no ``dbt ls``.

    Args:
        dbt_obj (Optional[DBTSelector]): Selector usado quando a materialização roda via objeto.
        dbt_command (Optional[Union[str, list[str]]]): Comando customizado; quando for uma lista
            iniciada por ``run``, os argumentos após ``run`` são a seleção.

    Returns:
        list[str]: Argumentos de seleção, ou lista vazia quando não há seleção aplicável.
    """
    if isinstance(dbt_command, list) and dbt_command and dbt_command[0] == "run":
        return dbt_command[1:]
    if isinstance(dbt_obj, DBTSelector):
        return ["--selector", dbt_obj.name]
    return []


def get_missing_dbt_relations(nodes: list[dict]) -> list[dict]:
    """
    Filtra os nós dbt cuja relação ainda não existe no BigQuery.

    Considera apenas materializações que geram relação (incremental, materialized_view,
    table, view) e consulta as tabelas existentes por dataset (``database.schema``),
    comparando pelo ``alias`` de cada nó.

    Args:
        nodes (list[dict]): Nós retornados por ``dbt ls --output json`` (com ``database``,
            ``schema``, ``alias`` e ``config``).

    Returns:
        list[dict]: Subconjunto de ``nodes`` cujas relações não existem no target.
    """
    relation_nodes = [
        node
        for node in nodes
        if node.get("config", {}).get("materialized")
        in {"incremental", "materialized_view", "table", "view"}
    ]
    nodes_by_dataset = defaultdict(list)
    for node in relation_nodes:
        nodes_by_dataset[(node["database"], node["schema"])].append(node)

    missing_nodes = []
    for (database, schema), dataset_nodes in nodes_by_dataset.items():
        client = bigquery.Client(project=database)
        try:
            existing_relations = {
                table.table_id for table in client.list_tables(f"{database}.{schema}")
            }
        except NotFound:
            existing_relations = set()

        missing_nodes.extend(
            node for node in dataset_nodes if node["alias"] not in existing_relations
        )

    return missing_nodes


def run_dbt_empty_for_missing_relations(
    dbt_obj: Optional[DBTSelector] = None,
    dbt_command: Optional[list[str]] = None,
    dbt_vars: Optional[dict] = None,
    flags: Optional[list[str]] = None,
    env: Optional[str] = None,
) -> None:
    """
    Cria relações dbt ausentes com ``--empty`` antes da materialização normal.

    O ``--empty`` é executado somente para modelos selecionados cuja materialização
    gera uma relação (incremental, table ou view) e que ainda não existem no target.
    Relações existentes não são executadas, evitando que lógica incremental consulte
    entradas vazias durante o pre-run.
    """
    if env != "dev":
        return

    selection_args = get_dbt_selection_args(dbt_obj=dbt_obj, dbt_command=dbt_command)
    if not selection_args:
        return

    flags = [flag for flag in (flags or []) if flag != "--full-refresh"]
    has_target_flag = "--target" in flags
    target = flags[flags.index("--target") + 1] if has_target_flag else get_dbt_target(env)
    if target == "prod":
        return

    project_dir, profiles_dir, target_path = get_dbt_paths()

    dbt_vars = dbt_vars or {}
    dbt_vars["flow_name"] = runtime.flow_run.flow_name
    vars_yaml = yaml.safe_dump(dbt_vars, default_flow_style=True)

    invoke = [
        "ls",
        *selection_args,
        "--resource-type",
        "model",
        "--output",
        "json",
        "--output-keys",
        "unique_id",
        "fqn",
        "database",
        "schema",
        "alias",
        "config",
        "--vars",
        vars_yaml,
    ]
    if not has_target_flag:
        invoke.extend(["--target", target])
    invoke.extend(flags)

    os.environ["DBT_PROJECT_DIR"] = str(project_dir)
    os.environ["DBT_PROFILES_DIR"] = str(profiles_dir)
    os.environ["DBT_TARGET_PATH"] = str(target_path)
    os.environ.setdefault("DBT_USER", "prefect")

    result = PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            target_path=target_path,
        ),
        raise_on_failure=False,
        _disable_callbacks=True,  # suprime o print de cada nó JSON no stdout
    ).invoke(invoke)
    if not result.success:
        raise ValueError(f"Falha ao listar modelos dbt: {result.exception}")

    nodes = [json.loads(line) for line in result.result]
    missing_nodes = get_missing_dbt_relations(nodes)

    if not missing_nodes:
        print("Nenhuma relação dbt ausente encontrada. Pulando pre-run com --empty.")
        return

    missing_selectors = [".".join(node["fqn"]) for node in missing_nodes]
    missing_relations = [
        f"{node['database']}.{node['schema']}.{node['alias']}" for node in missing_nodes
    ]
    print(f"Relações dbt ausentes no target {target}: {', '.join(missing_relations)}")
    run_dbt(
        dbt_command=["run", "--select", *missing_selectors],
        dbt_vars=dbt_vars,
        flags=[*flags, "--empty"],
        env=env,
    )


def run_dbt(  # noqa: PLR0913
    dbt_obj: Optional[Union[DBTSelector, DBTTest]] = None,
    dbt_command: Optional[Union[str, list[str]]] = None,
    dbt_vars: Optional[dict] = None,
    flags: Optional[list[str]] = None,
    raise_on_failure=True,
    is_snapshot: bool = False,
    env: Optional[str] = None,
):
    """
    Executa comandos do DBT e retorna os logs gerados.

    Args:
        dbt_obj (Optional[Union[DBTSelector, DBTTest]]): Objeto DBT a ser executado.
        dbt_command (Optional[Union[str, list[str]]]): Comando customizado. String para
            atalhos conhecidos (ex: "source freshness") ou lista para invocações livres
            (ex: ["run", "--select", "model", "--exclude", "other"]).
        dbt_vars (Optional[dict]): Variáveis para execução do DBT.
        flags (Optional[list[str]]): Flags adicionais do DBT.
        raise_on_failure (bool): Indica se deve lançar erro em falha.
        is_snapshot (bool): Se True, executa 'dbt snapshot' ao invés de 'dbt run'.
        env (Optional[str]): Ambiente de execução (prod ou dev). Define o target do dbt.

    Returns:
        str: Conteúdo do arquivo de log do DBT.
    """
    project_dir, profiles_dir, target_path = get_dbt_paths()
    flags = flags or []
    has_target_flag = "--target" in flags
    log_dir = f"{project_dir}/logs/{runtime.task_run.id}"

    flags = [
        *flags,
        "--log-path",
        log_dir,
        "--log-level-file",
        "info",
        "--log-format",
        "json",
    ]
    invoke = []
    if isinstance(dbt_command, list):
        invoke = dbt_command
    elif dbt_command == "source freshness":
        invoke = ["source", "freshness"]
    elif dbt_obj is not None:
        if isinstance(dbt_obj, DBTSelector):
            if is_snapshot:
                invoke = ["snapshot", "--selector", dbt_obj.name]
            else:
                invoke = ["run", "--selector", dbt_obj.name]
        elif isinstance(dbt_obj, DBTTest):
            invoke = ["test", "--select", dbt_obj.test_select]

    dbt_vars = dbt_vars or {}

    dbt_vars["flow_name"] = runtime.flow_run.flow_name

    vars_yaml = yaml.safe_dump(dbt_vars, default_flow_style=True)
    invoke = [*invoke, "--vars", vars_yaml]

    if env is not None and not has_target_flag:
        invoke = [*invoke, "--target", get_dbt_target(env)]

    invoke = invoke + flags
    print(f"Running DBT Command:\n{' '.join(invoke)}")
    os.environ["DBT_PROJECT_DIR"] = str(project_dir)
    os.environ["DBT_PROFILES_DIR"] = str(profiles_dir)
    os.environ["DBT_TARGET_PATH"] = str(target_path)
    os.environ.setdefault("DBT_USER", "prefect")

    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
            target_path=target_path,
        ),
        raise_on_failure=raise_on_failure,
    ).invoke(invoke)

    with (Path(log_dir) / "dbt.log").open("r") as logs:
        return logs.read()


def run_dbt_tests(  # noqa: PLR0913
    dbt_test: DBTTest,
    datetime_start: Optional[datetime],
    datetime_end: Optional[datetime],
    partitions: Optional[list[str]] = None,
    env: Optional[str] = None,
    flags: Optional[list[str]] = None,
) -> tuple[str, dict]:
    """
    Executa o DBT test

    Args:
        dbt_test (DBTTest): Objeto representando o teste do DBT.
        datetime_start (Optional[datetime]): Datetime inicial da execução.
        datetime_end (Optional[datetime]): Datetime final da execução.
        partitions (Optional[list[str]]): Lista de partições para execução dos testes.
        env (Optional[str]): Ambiente de execução (prod ou dev). Define o target do dbt.
        flags (Optional[list[str]]): Flags adicionais compatíveis com ``dbt test``.

    Returns:
        str: Logs da execução do DBT.
        dict: Dicionário contendo as variáveis utilizadas na execução do teste.
    """

    flags = [flag for flag in (flags or []) if flag not in {"--empty", "--full-refresh"}]
    if dbt_test.exclude is not None:
        flags += ["--exclude", dbt_test.exclude]
    dbt_vars = dbt_test.get_test_vars(
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        partitions=partitions,
    )
    log = run_dbt(dbt_obj=dbt_test, dbt_vars=dbt_vars, flags=flags, raise_on_failure=False, env=env)

    return log, dbt_vars


RELATION_RE = re.compile(r"`([^`]+)`\.`([^`]+)`\.`([^`]+)`")


def extract_relation_from_query(query: Optional[str]) -> Optional[str]:
    """
    Extrai nome completo da tabela (`project.dataset.table`) da primeira referência
    em uma query compilada do dbt.
    """
    if not query:
        return None
    match = RELATION_RE.search(query)
    if not match:
        return None
    return ".".join(match.groups())


def parse_dbt_test_output(dbt_logs: str) -> dict:
    """
    Processa os logs do DBT e extrai os resultados dos testes executados.

    Args:
        dbt_logs (str): Logs do DBT em formato texto JSON.

    Returns:
        dict: Resultados dos testes com status e queries associadas.
    """

    log_lines = re.split(r"(?m)(?=^)", dbt_logs)

    results = {}
    root_path = get_project_root_path()
    queries_path = filepath = root_path / "queries"

    for line in log_lines:
        if line.strip() == "":
            continue
        log_line_json = json.loads(line)
        data = log_line_json["data"]

        node_info = data.get("node_info", {})
        if node_info.get("materialized", "") == "test":
            test_name = node_info["node_name"]
            status = data.get("status")
            if status is not None:
                results[test_name] = {"result": status.upper()}

            path = data.get("path")

            if (
                path is not None
                and "compiled code at" in log_line_json.get("info", {}).get("msg", "").lower()
            ):
                filepath = queries_path / Path(os.path.relpath(path, queries_path))
                filepath = filepath.resolve()
                with filepath.open("r") as f:
                    query = f.read()

                query = re.sub(r"\n+", "\n", query)
                results[test_name]["query"] = query

    log_message = ""
    for test, info in results.items():
        result = info["result"]
        log_message += f"Test: {test} Status: {result}\n"

        if result in ("FAIL", "WARN"):
            log_message += "Query:\n"
            log_message += f"{info['query']}\n"

        if result == "ERROR":
            log_message += f"Error: {info['error']}\n"

        log_message += "\n"

    print(log_message)

    return results


class DBTTestFailedError(Exception): ...


class IncompleteDataError(Exception): ...


def rename_treatment_flow_run() -> str:
    """
    Gera o nome para execução de flows de tratamento.

    Returns:
        str: Nome para execução do flow.
    """
    return rename_flow_run()


def dbt_test_notify_discord(  # noqa: PLR0912, PLR0913, PLR0915
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
    if dbt_logs is None:
        return

    test_descriptions = dbt_test.test_descriptions

    checks_results = parse_dbt_test_output(dbt_logs)

    webhook_url = get_env_secret(secret_path=smtr_constants.WEBHOOKS_SECRET_PATH)[webhook_key]
    additional_mentions = additional_mentions or []
    mentions = [*additional_mentions, "dados_smtr"]
    mention_tags = "".join(
        [f" - <@&{smtr_constants.OWNERS_DISCORD_MENTIONS[m]['user_id']}>\n" for m in mentions]
    )

    test_check = all(test["result"] in ("PASS", "WARN") for test in checks_results.values())
    has_warn = any(test["result"] == "WARN" for test in checks_results.values())

    keys = [
        ("date_range_start", "date_range_end"),
        ("start_date", "end_date"),
        ("run_date", None),
        ("data_versao_gtfs", None),
    ]

    start_date = None
    end_date = None

    for start_key, end_key in keys:
        if start_key in dbt_vars and "T" in dbt_vars[start_key]:
            start_date = dbt_vars[start_key].split("T")[0]

            if end_key and end_key in dbt_vars and "T" in dbt_vars[end_key]:
                end_date = dbt_vars[end_key].split("T")[0]

            break
        elif start_key in dbt_vars:
            start_date = dbt_vars[start_key]

            if end_key and end_key in dbt_vars:
                end_date = dbt_vars[end_key]

    date_range = (
        start_date
        if not end_date
        else (start_date if start_date == end_date else f"{start_date} a {end_date}")
    )

    status_circle = (
        ":red_circle: "
        if not test_check
        else (":yellow_circle: " if has_warn else ":green_circle: ")
    )
    if "(target='dev')" in dbt_logs or "(target='hmg')" in dbt_logs:
        formatted_messages = [
            status_circle,
            f"**[DEV] Data Quality Checks - {runtime.flow_run.flow_name} - {date_range}**\n\n",
        ]
    else:
        formatted_messages = [
            status_circle,
            f"**Data Quality Checks - {runtime.flow_run.flow_name} - {date_range}**\n\n",
        ]

    table_groups = {}

    for test_id, test_result in checks_results.items():
        parts = test_id.split("__")
        if len(parts) >= 3:  # noqa: PLR2004
            table_name = parts[2]
        elif len(parts) == 2:  # noqa: PLR2004
            table_name = parts[1]
        else:
            table_name = parts[0]

        if table_name not in table_groups:
            table_groups[table_name] = []

        table_groups[table_name].append((test_id, test_result))

    for table_name, tests in table_groups.items():
        formatted_messages.append(f"*{table_name}:*\n")

        for test_id, test_result in tests:
            matched_description = None

            for key, value in test_descriptions.items():
                # singular: {test_id: {"description": ...}}
                if "description" in value:
                    if key == test_id:
                        matched_description = value["description"]
                        break
                    continue

                # grupo por tabela: {table_name: {test_id: {"description": ...}}}
                if table_name in key:
                    for existing_test_id, test_info in value.items():
                        if existing_test_id in test_id:
                            column = test_id.split("__")[1] if "__" in test_id else test_id
                            matched_description = test_info.get("description", test_id).replace(
                                "{column_name}", column
                            )
                            break
                    if matched_description:
                        break

            test_id = test_id.replace("_", "\\_")  # noqa: PLW2901
            description = matched_description or f"Teste: {test_id}"

            result_icon = {
                "PASS": ":white_check_mark:",
                "WARN": ":warning:",
            }.get(test_result["result"], ":x:")
            test_message = f"{result_icon} {description}\n"
            formatted_messages.append(test_message)

    formatted_messages.append("\n")
    if not test_check:
        status_message = (
            ":warning: **Status:** Testes falharam. Necessidade de revisão dos dados finais!\n"
        )
    elif has_warn:
        status_message = ":warning: **Status:** Sucesso com avisos. Verificar testes em WARN."
    else:
        status_message = ":tada: **Status:** Sucesso"
    formatted_messages.append(status_message)

    if not test_check:
        formatted_messages.append(mention_tags)

    try:
        format_send_discord_message(formatted_messages, webhook_url)
    except Exception as e:
        print(f"Falha ao enviar mensagem para o Discord: {e}")
        raise

    if not test_check and raise_check_error:
        raise DBTTestFailedError()


def get_deployment_commit_sha() -> Optional[str]:
    """
    Resolve o SHA completo do commit que gerou o deployment em execução.

    Usa ``runtime.deployment.version`` (que, por convenção dos ``prefect.yaml``, termina
    no short hash do commit do build) e resolve para o SHA completo via API pública do
    GitHub.

    Returns:
        Optional[str]: SHA completo do commit do deploy, ou None se não houver deployment
        em contexto ou se a resolução falhar (cai no comportamento padrão de clonar a
        branch default).
    """
    try:
        version = runtime.deployment.version
    except AttributeError:
        version = None
    if not version:
        return None

    short_sha = version.rsplit("-", 1)[-1]
    try:
        response = requests.get(f"{constants.REPO_URL}/commits/{short_sha}", timeout=60)
        response.raise_for_status()
        return response.json()["sha"]
    except (requests.RequestException, KeyError, ValueError) as exc:
        print(f"Não foi possível resolver o SHA do deployment '{version}': {exc}")
        return None


def clone_queries_from_github(env: str) -> Path:
    """
    Faz sparse-checkout apenas da pasta queries/ do repositório GitHub.

    Se estiver rodando localmente, retorna o caminho existente sem clonar. Em deploy
    de dev, fixa o ``queries/`` no commit que gerou o deployment (mesmo código da
    imagem) quando o SHA é resolvível. Em prod, usa a versão mais recente da branch
    default do repositório, permitindo atualizar queries sem redeploy dos flows.

    Args:
        env (str): Ambiente de execução, ``prod`` ou ``dev``.

    Returns:
        Path: Caminho para a pasta queries/ no projeto.
    """
    root_path = get_project_root_path()
    queries_path = root_path / "queries"

    if is_running_locally():
        if not queries_path.is_dir():
            raise FileNotFoundError(f"Pasta queries/ não encontrada: {queries_path}")
        print(f"Local: usando queries/ existente em {queries_path}")
        return queries_path

    git_bin = shutil.which("git")
    if git_bin is None:
        raise RuntimeError("Executável git não encontrado no ambiente.")

    repo_url = (
        f"{constants.REPO_URL.replace('https://api.github.com/repos/', 'https://github.com/')}.git"
    )
    commit_sha = get_deployment_commit_sha() if env == "dev" else None
    print(f"Clonando queries/ de {repo_url}...")
    with tempfile.TemporaryDirectory() as tmpdir:
        subprocess.run(
            [
                git_bin,
                "clone",
                "--depth",
                "1",
                "--filter=blob:none",
                "--no-checkout",
                repo_url,
                tmpdir,
            ],
            check=True,
            timeout=120,
        )
        subprocess.run(
            [git_bin, "sparse-checkout", "init", "--cone"], cwd=tmpdir, check=True, timeout=30
        )
        subprocess.run(
            [git_bin, "sparse-checkout", "set", "queries"], cwd=tmpdir, check=True, timeout=30
        )
        if commit_sha:
            print(f"Fixando queries/ no commit do deploy: {commit_sha}")
            subprocess.run(
                [git_bin, "fetch", "--depth", "1", "--filter=blob:none", "origin", commit_sha],
                cwd=tmpdir,
                check=True,
                timeout=120,
            )
        checkout_cmd = [git_bin, "checkout", commit_sha or "HEAD"]
        subprocess.run(checkout_cmd, cwd=tmpdir, check=True, timeout=60)
        if queries_path.exists():
            shutil.rmtree(queries_path)
        shutil.copytree(Path(tmpdir) / "queries", queries_path)

    print(f"queries/ clonado em {queries_path}")
    return queries_path


def run_dbt_deps() -> None:
    """
    Executa dbt deps para instalar pacotes.

    Se estiver rodando localmente e dbt_packages/ já existir, pula a execução.
    """
    root_path = get_project_root_path()
    project_dir = root_path / "queries"

    if is_running_locally() and (project_dir / "dbt_packages").exists():
        print("Local: dbt_packages/ já existe, pulando dbt deps.")
        return

    profiles_dir = project_dir / "dev" if is_running_locally() else project_dir
    print("Executando dbt deps...")
    PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=project_dir,
            profiles_dir=profiles_dir,
        ),
        raise_on_failure=True,
    ).invoke(["deps"])
