# -*- coding: utf-8 -*-
"""Utilidades para ler metadados dos deployments dos flows (prefect.yaml)"""

from importlib.resources import files
from pathlib import Path
from typing import Optional

import yaml

from pipelines import common


def get_flow_folder_path(flow_folder_name: str) -> Path:
    """
    Retorna o caminho da pasta de um flow a partir do seu nome.

    Args:
        flow_folder_name (str): Nome da pasta do flow (ex: capture__jae_auxiliar).

    Returns:
        Path: Caminho da pasta do flow.
    """
    return Path(files(common)).parent / flow_folder_name


def get_flow_schedule_cron(
    flow_folder_path: Path,
    table_id: Optional[str] = None,
) -> Optional[str]:
    """
    Retorna o cron do schedule do deployment prod de um flow.

    Lê o prefect.yaml da pasta do flow e busca os schedules do deployment
    `rj-<flow_name>--prod`. Se `table_id` for informado, prioriza o schedule cujo
    parâmetro `source_table_ids` contém o table_id; caso contrário, retorna o cron
    do primeiro schedule.

    Args:
        flow_folder_path (Path): Caminho da pasta do flow.
        table_id (Optional[str]): table_id usado para selecionar o schedule.

    Returns:
        Optional[str]: Expressão cron do schedule ou None se não houver.
    """
    flow_name = flow_folder_path.name

    with (flow_folder_path / "prefect.yaml").open("r") as f:
        prefect_file = yaml.safe_load(f)

    schedules = next(
        d
        for d in prefect_file["deployments"]
        if d["name"] == f"rj-{flow_name.replace('__', '--', 1)}--prod"
    ).get("schedules", [{}])

    if table_id is not None:
        for sched in schedules:
            ids = (sched.get("parameters") or {}).get("source_table_ids") or []
            if table_id in ids:
                return sched.get("cron")

    return schedules[0].get("cron")
