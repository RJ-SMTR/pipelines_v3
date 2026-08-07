# -*- coding: utf-8 -*-
"""Compara execuções do piloto OpenMetadata usando dados do Prefect.

Este script é somente de leitura: ele não cria nem cancela flow runs.

Exemplos:
    python scripts/benchmark_openmetadata_pilot.py baseline
    python scripts/benchmark_openmetadata_pilot.py compare <flow-run-id>
"""

import argparse
import asyncio
import math
import statistics
from datetime import datetime, timedelta
from typing import Optional
from uuid import UUID

from prefect.client.orchestration import get_client
from prefect.client.schemas.filters import (
    FlowRunFilter,
    FlowRunFilterDeploymentId,
    FlowRunFilterEndTime,
    FlowRunFilterState,
    FlowRunFilterStateType,
    LogFilter,
    LogFilterFlowRunId,
    TaskRunFilter,
    TaskRunFilterFlowRunId,
)
from prefect.client.schemas.objects import FlowRun, StateType, TaskRun
from prefect.client.schemas.sorting import FlowRunSort

DEFAULT_DEPLOYMENT = (
    "treatment--riorotativo-ordem-pagamento/rj-treatment--riorotativo_ordem_pagamento--prod"
)
OPENMETADATA_LOG_PREFIX = "OpenMetadata:"


def _seconds(value: Optional[timedelta]) -> Optional[float]:
    return value.total_seconds() if value is not None else None


def _execution_seconds(run: FlowRun | TaskRun) -> Optional[float]:
    duration = _seconds(run.total_run_time)
    if duration is not None:
        return duration
    if run.start_time is not None and run.end_time is not None:
        return (run.end_time - run.start_time).total_seconds()
    return None


def _startup_seconds(run: FlowRun) -> Optional[float]:
    if run.start_time is None or run.expected_start_time is None:
        return None
    return max(0.0, (run.start_time - run.expected_start_time).total_seconds())


def _end_to_end_seconds(run: FlowRun) -> Optional[float]:
    if run.end_time is None or run.expected_start_time is None:
        return None
    return max(0.0, (run.end_time - run.expected_start_time).total_seconds())


def _format_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "n/d"
    total_seconds = round(seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, remaining_seconds = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {remaining_seconds:02d}s"
    return f"{minutes}m {remaining_seconds:02d}s"


def _format_datetime(value: Optional[datetime]) -> str:
    return value.isoformat(timespec="seconds") if value is not None else "n/d"


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[index]


def _print_runs(runs: list[FlowRun]) -> None:
    print("\nExecuções consideradas:")
    print(
        "run_id                               início                     flow       espera     total"
    )
    for run in runs:
        print(
            f"{run.id}  {_format_datetime(run.start_time):25} "
            f"{_format_duration(_execution_seconds(run)):10} "
            f"{_format_duration(_startup_seconds(run)):10} "
            f"{_format_duration(_end_to_end_seconds(run)):10}"
        )


def _print_summary(runs: list[FlowRun]) -> Optional[float]:
    durations = [duration for run in runs if (duration := _execution_seconds(run)) is not None]
    if not durations:
        print("Nenhuma execução concluída com duração disponível.")
        return None

    median = statistics.median(durations)
    print("\nResumo da duração do flow:")
    print(f"  amostra: {len(durations)}")
    print(f"  média:   {_format_duration(statistics.mean(durations))}")
    print(f"  mediana: {_format_duration(median)}")
    print(f"  p90:     {_format_duration(_percentile(durations, 0.90))}")
    print(f"  mínimo:  {_format_duration(min(durations))}")
    print(f"  máximo:  {_format_duration(max(durations))}")
    return median


async def _read_completed_runs(
    deployment_name: str,
    limit: int,
    excluded_run_id: Optional[UUID] = None,
    ended_before: Optional[datetime] = None,
) -> list[FlowRun]:
    async with get_client() as client:
        deployment = await client.read_deployment_by_name(deployment_name)
        runs = await client.read_flow_runs(
            flow_run_filter=FlowRunFilter(
                deployment_id=FlowRunFilterDeploymentId(any_=[deployment.id]),
                state=FlowRunFilterState(type=FlowRunFilterStateType(any_=[StateType.COMPLETED])),
                end_time=(
                    FlowRunFilterEndTime(before_=ended_before) if ended_before is not None else None
                ),
            ),
            sort=FlowRunSort.END_TIME_DESC,
            limit=limit + (1 if excluded_run_id else 0),
        )
    if excluded_run_id is not None:
        runs = [run for run in runs if run.id != excluded_run_id]
    return runs[:limit]


async def _read_openmetadata_details(flow_run_id: UUID) -> tuple[FlowRun, list[TaskRun], list[str]]:
    async with get_client() as client:
        run = await client.read_flow_run(flow_run_id)
        task_runs = await client.read_task_runs(
            task_run_filter=TaskRunFilter(flow_run_id=TaskRunFilterFlowRunId(any_=[flow_run_id]))
        )
        logs = await client.read_logs(
            log_filter=LogFilter(flow_run_id=LogFilterFlowRunId(any_=[flow_run_id])),
            limit=10_000,
        )

    openmetadata_tasks = [
        task_run for task_run in task_runs if "openmetadata" in task_run.name.lower()
    ]
    openmetadata_logs = [log.message for log in logs if OPENMETADATA_LOG_PREFIX in log.message]
    return run, openmetadata_tasks, openmetadata_logs


def _classify_openmetadata(logs: list[str]) -> str:
    if any("ingestão concluída com sucesso" in message for message in logs):
        return "ingestão direta concluída"
    if any("artefatos enviados para gs://" in message for message in logs):
        return "falha direta; fallback enviado ao GCS"
    if any("ingestão ignorada no ambiente" in message for message in logs):
        return "ingestão ignorada pelo ambiente"
    if any("falha não fatal" in message for message in logs):
        return "falha não fatal sem confirmação de fallback"
    return "resultado não identificado nos logs"


async def _baseline(args: argparse.Namespace) -> None:
    runs = await _read_completed_runs(args.deployment, args.limit)
    print(f"Deployment: {args.deployment}")
    _print_runs(runs)
    _print_summary(runs)


async def _compare(args: argparse.Namespace) -> None:
    flow_run_id = UUID(args.flow_run_id)
    run, openmetadata_tasks, openmetadata_logs = await _read_openmetadata_details(flow_run_id)
    baseline_runs = await _read_completed_runs(
        args.deployment,
        args.limit,
        excluded_run_id=flow_run_id,
        ended_before=run.start_time or run.expected_start_time,
    )
    baseline_median = _print_summary(baseline_runs)
    pilot_duration = _execution_seconds(run)

    print("\nRun piloto:")
    print(f"  id:                  {run.id}")
    print(f"  estado:              {run.state_name}")
    print(f"  início:              {_format_datetime(run.start_time)}")
    print(f"  duração do flow:     {_format_duration(pilot_duration)}")
    print(f"  espera/inicialização:{_format_duration(_startup_seconds(run)):>11}")
    print(f"  ponta a ponta:       {_format_duration(_end_to_end_seconds(run))}")

    if baseline_median is not None and pilot_duration is not None:
        delta = pilot_duration - baseline_median
        percentage = (delta / baseline_median * 100) if baseline_median else 0.0
        print(f"  delta vs. mediana:   {delta:+.1f}s ({percentage:+.1f}%)")

    print("\nOpenMetadata:")
    print(f"  resultado: {_classify_openmetadata(openmetadata_logs)}")
    if openmetadata_tasks:
        for task_run in openmetadata_tasks:
            print(
                f"  task: {task_run.name} | estado={task_run.state_name} | "
                f"duração={_format_duration(_execution_seconds(task_run))}"
            )
    else:
        print("  task: não encontrada")

    if args.show_openmetadata_logs:
        print("\nLogs OpenMetadata:")
        for message in openmetadata_logs:
            print(f"  {message}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Benchmark somente de leitura do piloto OpenMetadata no Prefect."
    )
    parser.add_argument(
        "--deployment",
        default=DEFAULT_DEPLOYMENT,
        help="Nome completo FLOW/DEPLOYMENT no Prefect.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Quantidade de runs concluídas usadas como baseline (padrão: 10).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("baseline", help="Resume as últimas runs concluídas.")

    compare = subparsers.add_parser("compare", help="Compara uma run piloto com o histórico.")
    compare.add_argument("flow_run_id", help="UUID da flow run piloto.")
    compare.add_argument(
        "--show-openmetadata-logs",
        action="store_true",
        help="Exibe somente as linhas de log prefixadas com OpenMetadata.",
    )
    return parser


async def _main() -> None:
    args = _parser().parse_args()
    if args.limit < 1:
        raise ValueError("--limit deve ser maior que zero")
    if args.command == "baseline":
        await _baseline(args)
    else:
        await _compare(args)


if __name__ == "__main__":
    asyncio.run(_main())
