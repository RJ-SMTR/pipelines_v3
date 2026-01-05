# -*- coding: utf-8 -*-
# flake8: noqa
import json
import os
import re
import subprocess
from pathlib import Path
from typing import Dict, List, Union

import basedosdados as bd
import requests
from prefect import context

# from datetime import datetime as dt
# from datetime import timedelta
from pipelines.constants import constants as smtr_constants
from pipelines.utils.discord import format_send_discord_message
from pipelines.utils.secret import get_secret

# import pandas as pd
bd.config.from_file = True


def run_dbt(
    resource: str,
    selector_name: str = None,
    dataset_id: str = None,
    table_id: str = None,
    model: str = None,
    upstream: bool = None,
    downstream: bool = None,
    test_name: str = None,
    exclude: str = None,
    flags: str = None,
    _vars: dict | list[dict] = None,
) -> str:
    """
    Generic task to run different DBT resources (run, snapshot, test).

    Args:
        resource (str): The DBT resource type to run ('selector', 'snapshot', or 'test').
        selector_name (str, optional): The name of the selector or snapshot to run.
        dataset_id (str, optional): Dataset ID of the dbt model. Used for test resource.
        table_id (str, optional): Table ID of the dbt model. Used for test resource.
        model (str, optional): Specific model to be tested. Used for test resource.
        upstream (bool, optional): If True, includes upstream models. Used for test resource.
        downstream (bool, optional): If True, includes downstream models. Used for test resource.
        test_name (str, optional): The name of the test to be executed. Used for test resource.
        exclude (str, optional): Models to be excluded from the execution. Used for test resource.
        flags (str, optional): Flags to pass to the dbt command.
        _vars (Union[dict, list[dict]], optional): Variables to pass to dbt.

    Returns:
        str: Output logs from the DBT command execution.
    """

    resource_mapping = {
        "model": "run",
        "snapshot": "snapshot",
        "test": "test",
        "source freshness": "source freshness",
    }

    if resource not in resource_mapping:
        raise ValueError(
            f"Invalid resource: {resource}. Must be one of {list(resource_mapping.keys())}"
        )

    dbt_command = resource_mapping[resource]

    run_command = f"dbt {dbt_command}"

    if resource in ["model", "snapshot"]:
        if not selector_name:
            raise ValueError(f"selector_name is required for resource type: {resource}")
        run_command += f" --selector {selector_name}"
    elif resource == "test":
        run_command += " --select "

        if test_name:
            run_command += test_name
        else:
            if not model and dataset_id:
                model = dataset_id
                if table_id:
                    model += f".{table_id}"

            if model:
                if upstream:
                    run_command += "+"
                run_command += model
                if downstream:
                    run_command += "+"

    if exclude:
        run_command += f" --exclude {exclude}"

    if _vars:
        if isinstance(_vars, list):
            vars_dict = {}
            for elem in _vars:
                vars_dict.update(elem)
            vars_str = f'"{vars_dict}"'
            run_command += f" --vars {vars_str}"
        else:
            vars_str = f'"{_vars}"'
            run_command += f" --vars {vars_str}"

    if flags:
        run_command += f" {flags}"

    run_command += f" -x --profiles-dir ./dev"

    print(f"Running dbt with command: {run_command}")
    os.chdir(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    return os.system(run_command)


def run_dbt_model(
    dataset_id: str = None,
    table_id: str = None,
    model: str = None,
    upstream: bool = None,
    downstream: bool = None,
    exclude: str = None,
    flags: str = None,
    _vars: Union[dict, List[Dict]] = None,
):
    """
    Run a DBT model.
    """
    run_command = "dbt run"

    common_flags = "-x --profiles-dir ./dev"

    if not flags:
        flags = common_flags
    else:
        flags = common_flags + " " + flags

    if not model:
        model = f"{dataset_id}"
        if table_id:
            model += f".{table_id}"

    # Set models and upstream/downstream for dbt
    if model:
        run_command += " --select "
        if upstream:
            run_command += "+"
        run_command += f"{model}"
        if downstream:
            run_command += "+"

    if exclude:
        run_command += f" --exclude {exclude}"

    if _vars:
        if isinstance(_vars, list):
            vars_dict = {}
            for elem in _vars:
                vars_dict.update(elem)
            vars_str = f'"{vars_dict}"'
            run_command += f" --vars {vars_str}"
        else:
            vars_str = f'"{_vars}"'
            run_command += f" --vars {vars_str}"
    if flags:
        run_command += f" {flags}"

    print(f"\n>>> RUNNING: {run_command}\n")
    os.chdir(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    return os.system(run_command)


def run_dbt_selector(
    selector_name: str,
    flags: str = None,
    _vars: dict | list[dict] = None,
):
    """
    Runs a DBT selector.

    Args:
        selector_name (str): The name of the DBT selector to run.
        flags (str, optional): Flags to pass to the dbt run command.
        _vars (Union[dict, list[dict]], optional): Variables to pass to dbt. Defaults to None.
    """
    # Build the dbt command
    run_command = f"dbt run --selector {selector_name}"

    if _vars:
        if isinstance(_vars, list):
            vars_dict = {}
            for elem in _vars:
                vars_dict.update(elem)
            vars_str = f'"{vars_dict}"'
            run_command += f" --vars {vars_str}"
        else:
            vars_str = f'"{_vars}"'
            run_command += f" --vars {vars_str}"

    if flags:
        run_command += f" {flags}"

    print(f"\n>>> RUNNING: {run_command}\n")
    os.chdir(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    os.system(run_command)


def fetch_dataset_sha(dataset_id: str):
    """Fetches the SHA of a branch from Github"""
    url = "https://api.github.com/repos/prefeitura-rio/queries-rj-smtr"
    url += f"/commits?queries-rj-smtr/rj_smtr/{dataset_id}"
    response = requests.get(url)

    if response.status_code != 200:
        return None

    dataset_version = response.json()[0]["sha"]
    return {"version": dataset_version}


def run_dbt_tests(
    dataset_id: str = None,
    table_id: str = None,
    model: str = None,
    upstream: bool = None,
    downstream: bool = None,
    test_name: str = None,
    exclude: str = None,
    flags: str = None,
    _vars: Union[dict, List[Dict]] = None,
):
    """
    Run DBT test
    """
    run_command = "dbt test"

    common_flags = "--profiles-dir ./dev"

    if flags:
        flags = f"{common_flags} {flags}"
    else:
        flags = common_flags

    if not model:
        model = dataset_id
        if table_id:
            model += f".{table_id}"

    if model:
        run_command += " --select "
        if upstream:
            run_command += "+"
        run_command += model
        if downstream:
            run_command += "+"
        if test_name:
            model += f",test_name:{test_name}"

    if exclude:
        run_command += f" --exclude {exclude}"

    if _vars:
        if isinstance(_vars, list):
            vars_dict = {}
            for elem in _vars:
                vars_dict.update(elem)
            vars_str = f'"{vars_dict}"'
            run_command += f" --vars {vars_str}"
        else:
            vars_str = f'"{_vars}"'
            run_command += f" --vars {vars_str}"

    if flags:
        run_command += f" {flags}"

    print(f"\n>>> RUNNING: {run_command}\n")

    project_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
    os.chdir(project_dir)
    dbt_logs = subprocess.run(run_command, shell=True, capture_output=True, text=True)

    print(dbt_logs.stdout)
    return dbt_logs.stdout


def parse_dbt_test_output(dbt_logs: str) -> dict:
    # Remover sequências ANSI
    dbt_logs = re.sub(r"\x1B[@-_][0-?]*[ -/]*[@-~]", "", dbt_logs)

    results = {}
    result_pattern = r"\d+ of \d+ (PASS|FAIL|ERROR) (\d+ )?([\w_]+) .* \[(PASS|FAIL|ERROR) .*\]"
    fail_pattern = r"Failure in test ([\w_]+) .*\n.*\n.*\n.* compiled Code at (.*)\n"
    error_pattern = r"Error in test ([\w_]+) \(.*schema.yaml\)\n  (.*)\n"

    for match in re.finditer(result_pattern, dbt_logs):
        groups = match.groups()
        test_name = groups[2]
        results[test_name] = {"result": groups[3]}

    for match in re.finditer(fail_pattern, dbt_logs):
        groups = match.groups()
        test_name = groups[0]
        file = groups[1]

        with open(file, "r") as arquivo:
            query = arquivo.read()

        query = re.sub(r"\n+", "\n", query)
        results[test_name]["query"] = query

    for match in re.finditer(error_pattern, dbt_logs):
        groups = match.groups()
        test_name = groups[0]
        error = groups[1]
        results[test_name]["error"] = error

    log_message = ""
    for test, info in results.items():
        result = info["result"]
        log_message += f"Test: {test} Status: {result}\n"

        if result == "FAIL":
            log_message += "Query:\n"
            log_message += f"{info['query']}\n"

        if result == "ERROR":
            log_message += f"Error: {info['error']}\n"

        log_message += "\n"

    print(log_message)

    return results


def dbt_data_quality_checks(
    checks_list: dict, checks_results: dict, params: dict, webhook_url: str = None
) -> bool:

    if webhook_url is None:
        webhook_url = get_secret(secret_path=smtr_constants.WEBHOOKS_SECRET_PATH.value)["dataplex"]

    dados_tag = f" - <@&{smtr_constants.OWNERS_DISCORD_MENTIONS.value['dados_smtr']['user_id']}>\n"

    test_check = all(test["result"] == "PASS" for test in checks_results.values())

    date_range = (
        params["date_range_start"]
        if params["date_range_start"] == params["date_range_end"]
        else f'{params["date_range_start"]} a {params["date_range_end"]}'
    )

    formatted_messages = [
        ":green_circle: " if test_check else ":red_circle: ",
        f"**[DEV]Data Quality Checks - {context.get('flow_name')} - {date_range}**\n\n",
    ]

    for table_id, tests in checks_list.items():
        formatted_messages.append(
            f"*{table_id}:*\n"
            + "\n".join(
                f'{":white_check_mark:" if checks_results[test_id]["result"] == "PASS" else ":x:"} '
                f'{test["description"]}'
                for test_id, test in tests.items()
            )
        )

    formatted_messages.append("\n\n")
    formatted_messages.append(
        ":tada: **Status:** Sucesso"
        if test_check
        else ":warning: **Status:** Testes falharam. Necessidade de revisão dos dados finais!\n"
    )

    formatted_messages.append(dados_tag)
    format_send_discord_message(formatted_messages, webhook_url)


def get_model_table_info(model_name):
    os.chdir(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
    target_path = Path("target/manifest.json")

    with target_path.open() as file:
        manifest = json.load(file)
    for _, node in manifest["nodes"].items():
        if node["resource_type"] == "model" and node["name"] == model_name:
            return {
                "project": node["database"],
                "dataset_id": node["schema"],
                "table_id": node["alias"],
            }

    raise ValueError("modelo não encontrado no arquivo manifest.json")


def test_incremental_model(
    model: str,
    _vars: dict,
    partition_column_name: str,
    expected_changed_partitions: list[str],
    unique_key: Union[str, list[str]] = None,
):
    model_info = get_model_table_info(model_name=model)
    changed_partitions_sql = f"""
            select
                cast(parse_date('%Y%m%d', partition_id) as string) as particao,
                last_modified_time
            from {model_info["project"]}.{model_info["dataset_id"]}.INFORMATION_SCHEMA.PARTITIONS
            where table_name = '{model_info["table_id"]}' and partition_id != "__NULL__"
        """
    changed_partitions_before = bd.read_sql(changed_partitions_sql).rename(
        columns={"last_modified_time": "last_modified_time_before"}
    )
    partition_filter = ",".join([f"'{p}'" for p in expected_changed_partitions])
    partition_count_sql = f"""
        select
            {partition_column_name} as particao,
            count(*) as qt_registros
        from {model_info["project"]}.{model_info["dataset_id"]}.{model_info["table_id"]}
        where {partition_column_name} in ({partition_filter})
        group by 1
    """

    partition_count_before = bd.read_sql(partition_count_sql).rename(
        columns={"qt_registros": "qt_registros_before"}
    )

    assert run_dbt_model(model=model, _vars=_vars) == 0, "DBT Falhou!"

    changed_partitions_after = bd.read_sql(changed_partitions_sql).rename(
        columns={"last_modified_time": "last_modified_time_after"}
    )

    partition_count_after = bd.read_sql(partition_count_sql).rename(
        columns={"qt_registros": "qt_registros_after"}
    )

    changed_partitions = changed_partitions_before.merge(changed_partitions_after, on="particao")
    changed_partitions = changed_partitions.loc[
        changed_partitions["last_modified_time_before"]
        != changed_partitions["last_modified_time_after"]
    ]
    changed_partitions = changed_partitions["particao"].to_list()
    not_expected_changed = [p for p in changed_partitions if p not in expected_changed_partitions]
    expected_not_changed = [p for p in expected_changed_partitions if p not in changed_partitions]

    if len(not_expected_changed) > 0:
        print(f"Foram alteradas partições não esperadas: {', '.join(not_expected_changed)}")
    if len(expected_not_changed) > 0:
        print(f"Não foram alteradas partições esperadas: {', '.join(expected_not_changed)}")

    changed_record_count = partition_count_before.merge(partition_count_after, on="particao")
    changed_record_count = changed_record_count.loc[
        changed_record_count["qt_registros_before"] != changed_record_count["qt_registros_after"]
    ]

    changed_record_count = changed_record_count.to_dict("records")

    if len(changed_record_count) > 0:
        print("Contagem de registros modificada:")
        print(
            "\n".join(
                [
                    f"{a['particao']}: {a['qt_registros_before']} > {a['qt_registros_after']}"
                    for a in changed_record_count
                ]
            )
        )

    if unique_key:
        join_pattern = ", '_', "
        unique_key = (
            unique_key
            if isinstance(unique_key, str)
            else f"concat({join_pattern.join(unique_key)})"
        )
        partition_filter = ",".join(
            [f"'{p}'" for p in changed_partitions_after["particao"].to_list()]
        )
        unique_count_sql = f"""
            select
                {unique_key} as unique_key,
                count(*) as qtd
            from {model_info["project"]}.{model_info["dataset_id"]}.{model_info["table_id"]}
            where {partition_column_name} in ({partition_filter})
            group by 1
            having count(*) > 1
        """
        unique_count = bd.read_sql(unique_count_sql)
        if len(unique_count) > 0:
            print("Linhas duplicadas:")
            print(unique_count)
