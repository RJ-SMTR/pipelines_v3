# -*- coding: utf-8 -*-
# /// script
# dependencies = [
#     "prefect-docker>=0.6.5",
#     "prefect>=3.4.3",
#     "pyyaml>=6.0.2",
#     "rich>=14.1.0",
#     "tenacity>=9.1.0",
#     "uvloop>=0.21.0"
# ]
# ///

import asyncio
import logging
import sys
from collections.abc import Awaitable, Iterable
from dataclasses import dataclass, field
from itertools import batched
from os import environ
from pathlib import Path
from typing import Callable, TypedDict

import uvloop
from rich.console import Console
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import Progress, TaskID
from rich.table import Table
from tenacity import retry, stop_after_attempt, wait_exponential
from yaml import safe_load

console = Console()

logging.basicConfig(
    level=environ.get("LOG_LEVEL", "INFO").upper(),
    format="%(message)s",
    handlers=[RichHandler(console=console, show_time=True, show_path=False)],
)

DOCKER_IMAGE_FORMAT_PARTS_COUNT = 2
BASE_IMAGE = "ghcr.io/rj-smtr/pipelines_v3:latest"

DeploymentFunc = Callable[[Path, str], Awaitable[tuple[Path, int]]]


def safe_int_from_env(key: str, default: int, min_value: int = 0) -> int:
    """Safely parse integer from environment variable with validation."""
    try:
        value = int(environ.get(key, str(default)))
        return max(min_value, value)
    except ValueError:
        print(f"Warning: Invalid value for {key}, using default: {default}", file=sys.stderr)
        return default


def safe_float_from_env(key: str, default: float, min_value: float = 0.0) -> float:
    """Safely parse float from environment variable with validation."""
    try:
        value = float(environ.get(key, str(default)))
        return max(min_value, value)
    except ValueError:
        print(f"Warning: Invalid value for {key}, using default: {default}", file=sys.stderr)
        return default


@dataclass
class DeploymentConfig:
    """Configuration for deployment script."""

    backoff_multiplier: float = field(
        default_factory=lambda: safe_float_from_env("BACKOFF_MULTIPLIER", 2.0, 1.0)
    )
    batch_size: int = field(default_factory=lambda: safe_int_from_env("BATCH_SIZE", 3, 1))
    cleanup_docker: bool = field(default_factory=lambda: environ.get("CLEANUP_DOCKER", "1") == "1")
    deployment_timeout: int = field(
        default_factory=lambda: safe_int_from_env("DEPLOYMENT_TIMEOUT", 600, 30)
    )
    environment: str = field(default_factory=lambda: environ.get("ENVIRONMENT", "staging"))
    force_deploy: bool = field(default_factory=lambda: environ.get("FORCE_DEPLOY", "0") == "1")
    max_retries: int = field(default_factory=lambda: safe_int_from_env("MAX_RETRIES", 2, 0))
    pipeline: str | None = field(default_factory=lambda: environ.get("PIPELINE"))
    pipelines_path: str = field(default_factory=lambda: environ.get("PIPELINES_PATH", "pipelines"))
    retry_delay: int = field(default_factory=lambda: safe_int_from_env("RETRY_DELAY", 30, 1))
    sha: str = field(default_factory=lambda: environ.get("GITHUB_SHA", "HEAD"))


CONFIG = DeploymentConfig()

deployment_results: dict[str, list[str]] = {"successful": [], "failed": []}


class WorkPool(TypedDict, total=False):
    """Work pool configuration."""

    name: str
    work_queue_name: str
    job_variables: dict[str, str | int | bool]


class Schedule(TypedDict, total=False):
    """Schedule configuration."""

    cron: str
    timezone: str
    active: bool


class BuildStep(TypedDict, total=False):
    """Build step configuration."""

    prefect_docker: dict[str, str]
    run_shell_script: dict[str, str]


class PrefectDeployment(TypedDict, total=False):
    """Represents a deployment configuration in prefect.yaml."""

    name: str
    entrypoint: str
    parameters: dict[str, str | int | bool | list[str]]
    work_pool: WorkPool
    schedule: Schedule


class PrefectYaml(TypedDict, total=False):
    """Represents the structure of a prefect.yaml file."""

    name: str
    prefect_version: str
    build: list[BuildStep]
    push: list[dict[str, str]]
    pull: list[dict[str, str]]
    deployments: list[PrefectDeployment]


async def get_deployments(prefect_yaml: Path) -> list[str]:
    """Extract deployment names from the given `prefect.yaml` file."""
    try:
        content: PrefectYaml = safe_load(prefect_yaml.read_text()) or {}
    except (OSError, IOError) as e:
        logging.error(f"Failed to read `{prefect_yaml}`: {e}")
        return []
    except Exception as e:
        logging.error(f"Failed to parse YAML in `{prefect_yaml}`: {e}")
        return []

    deployments_section: list[PrefectDeployment] = content.get("deployments", [])

    if not deployments_section:
        logging.warning(f"No deployments section found in `{prefect_yaml}`.")
        return []

    return [d.get("name", "") for d in deployments_section if d.get("name")]


def get_prefect_yaml_files(
    package_dir: Iterable[Path], pipeline_filter: str | None = None
) -> list[Path]:
    """Get all `prefect.yaml` files in the specified package directory, optionally filtered by pipeline name."""
    yaml_files = [
        d / "prefect.yaml" for d in package_dir if d.is_dir() and (d / "prefect.yaml").exists()
    ]

    if pipeline_filter:
        return [f for f in yaml_files if f.parent.name == pipeline_filter]

    return yaml_files


async def get_changed_directories(package_dir: Path, sha: str) -> list[Path]:
    """Get directories that have changed since the specified commit SHA."""
    command = ["git", "diff", "--name-only", f"{sha}^", sha, "--", package_dir.as_posix()]

    try:
        process = await run_subprocess(command)
        stdout, stderr = await process.communicate()

        if process.returncode != 0:
            logging.warning(f"Git command failed: {stderr.decode().strip()}")
            return []

        parsed_stdout = stdout.decode().strip().splitlines()
        logging.debug(f"Command: `{' '.join(command)}`")
        logging.debug(f"STDOUT: {parsed_stdout}")

        return list({Path(f).parent for f in parsed_stdout if f.strip()})
    except Exception as e:
        logging.error(f"Failed to get changed directories: {e}")
        return []


def parse_image_line(line: str) -> str | None:
    """Parse a Docker image line and return the image ID if valid."""
    parts = line.strip().split()
    return parts[1] if len(parts) >= DOCKER_IMAGE_FORMAT_PARTS_COUNT else None


async def get_docker_image_ids() -> list[str]:
    """Get list of Docker image IDs excluding the base image."""
    list_cmd = ["docker", "images", "--format", "{{.Repository}}:{{.Tag}} {{.ID}}"]

    process = await run_subprocess(list_cmd)

    stdout, stderr = await process.communicate()

    if process.returncode != 0:
        logging.warning(f"Failed to list Docker images: {stderr.decode().strip()}")
        return []

    images_output = stdout.decode().strip()

    if not images_output:
        return []

    filtered_lines = filter(
        lambda line: line.strip() and not line.startswith(BASE_IMAGE), images_output.splitlines()
    )

    parsed_ids = map(parse_image_line, filtered_lines)

    return list(filter(None, parsed_ids))


async def remove_docker_images(image_ids: list[str]) -> None:
    """Remove specified Docker images."""
    if not image_ids:
        return

    remove_cmd = ["docker", "rmi", "-f", *image_ids]

    try:
        process = await run_subprocess(remove_cmd)
        _, stderr = await process.communicate()

        if process.returncode != 0:
            logging.warning(f"Some images could not be removed: {stderr.decode().strip()}")
            return

        logging.info("Docker images cleaned up successfully")
    except Exception as e:
        logging.warning(f"Failed to remove Docker images: {e}")


async def cleanup_docker_system() -> None:
    """Clean up Docker build cache and unused resources."""
    commands = [["docker", "builder", "prune", "-f"], ["docker", "system", "prune", "-f"]]

    for cmd in commands:
        process = await run_subprocess(cmd)

        _ = await process.communicate()


def build_deployment_command(package: str, deployment: str, file_path: str) -> list[str]:
    """Build deployment command array."""
    return [
        "uv",
        "run",
        "--package",
        package,
        "--",
        "prefect",
        "--no-prompt",
        "deploy",
        "--name",
        deployment,
        "--prefect-file",
        file_path,
    ]


def is_retryable_error(stderr_text: str) -> bool:
    """Check if error is retryable based on stderr content."""
    return any(
        error in stderr_text.lower()
        for error in ["timeout", "connection", "network", "read timed out"]
    )


async def run_subprocess(command: list[str]) -> asyncio.subprocess.Process:
    """Run subprocess with standard configuration."""
    return await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


def validate_configuration() -> None:
    """Validate configuration for conflicting options."""
    if CONFIG.pipeline and CONFIG.force_deploy:
        logging.error("Cannot use both PIPELINE and FORCE_DEPLOY options simultaneously")
        logging.error(
            "Use PIPELINE for single pipeline deployment OR FORCE_DEPLOY for all pipelines"
        )
        sys.exit(1)


def validate_pipelines_path(pipelines_path: str) -> Path:
    """Validate and return pipelines path."""
    if not pipelines_path:
        logging.error("PIPELINES_PATH cannot be empty")
        sys.exit(1)

    pipelines = Path(pipelines_path)

    if not pipelines.exists():
        logging.error(f"Path `{pipelines}` does not exist.")
        sys.exit(1)

    if not pipelines.is_dir():
        logging.error(f"Path `{pipelines}` is not a directory.")
        sys.exit(1)

    return pipelines


async def discover_yaml_files(pipelines: Path, pipeline_filter: str | None) -> list[Path]:
    """Discover YAML files to deploy based on configuration."""
    if pipeline_filter:
        logging.debug(f"Pipeline: `{pipeline_filter}`")
        pipeline_path = pipelines / pipeline_filter

        if not pipeline_path.exists() or not pipeline_path.is_dir():
            logging.error(f"Pipeline `{pipeline_filter}` does not exist in `{pipelines}`")
            sys.exit(1)

        yamls = get_prefect_yaml_files([pipeline_path], pipeline_filter)

        if not yamls:
            logging.error(f"No prefect.yaml found in pipeline `{pipeline_filter}`")
            sys.exit(1)

        return yamls

    changed_pipelines = await get_changed_directories(pipelines, CONFIG.sha)

    if not changed_pipelines and not CONFIG.force_deploy:
        logging.info("No changes detected, skipping deployment.")
        sys.exit(0)

    return get_prefect_yaml_files(changed_pipelines if changed_pipelines else pipelines.iterdir())


async def execute_deployments(yamls: list[Path]) -> list[Path]:
    """Execute deployments in batches and return failed deployments."""
    all_errors: list[Path] = []
    batches = list(batched(yamls, CONFIG.batch_size, strict=False))

    with Progress(console=console) as progress:
        task_id: TaskID = progress.add_task("Deploying flows...", total=len(yamls))

        for batch_num, batch in enumerate(batches, 1):
            logging.info(f"Deploying batch {batch_num}/{len(batches)}: {[str(y) for y in batch]}")

            tasks = [deploy_flow(file, CONFIG.environment) for file in batch]
            result: list[tuple[Path, int]] = await asyncio.gather(*tasks)
            all_errors.extend(file for file, code in result if code != 0)

            progress.update(task_id, advance=len(batch))

            if CONFIG.cleanup_docker and batch_num < len(batches):
                await cleanup_docker_images()

    return all_errors


async def cleanup_docker_images() -> None:
    """Clean up Docker images except the base image to free disk space."""
    try:
        logging.info("Starting Docker cleanup to free disk space...")

        image_ids = await get_docker_image_ids()

        if not image_ids:
            logging.info("No Docker images to clean up")
            return

        logging.info(f"Removing {len(image_ids)} Docker images...")

        await remove_docker_images(image_ids)
        await cleanup_docker_system()

        logging.info("Docker cleanup completed")

    except Exception as e:
        logging.warning(f"Docker cleanup failed but continuing deployment: {e}")


@retry(
    stop=stop_after_attempt(CONFIG.max_retries + 1),
    wait=wait_exponential(
        multiplier=CONFIG.retry_delay,
        exp_base=CONFIG.backoff_multiplier,
        min=CONFIG.retry_delay,
        max=300,
    ),
)
async def deploy_flow(file: Path, environment: str) -> tuple[Path, int]:
    """Deploy a Prefect flow with retry logic."""
    package = file.parent.name

    if not file.exists():
        logging.error(f"File `{file}` does not exist.")
        return file, 1

    logging.info(f"Deploying `{package}` {environment} pipelines...")

    deployments = await get_deployments(file)

    try:
        deployment = next(d for d in deployments if d.endswith(environment))
    except StopIteration:
        logging.warning(
            f"No deployment found for `{package}` in `{environment}` environment. Skipping."
        )
        return file, 0

    command = build_deployment_command(package, deployment, str(file))

    process = await run_subprocess(command)

    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=CONFIG.deployment_timeout
        )
    except asyncio.TimeoutError:
        process.kill()
        _ = await process.wait()
        raise TimeoutError(f"Deployment timed out for `{package}`") from None

    if process.returncode != 0:
        stderr_text = stderr.decode().strip()

        if is_retryable_error(stderr_text):
            raise RuntimeError(f"Network error deploying `{package}`: {stderr_text}")

        logging.error(f"Failed to deploy `{package}` to {environment} environment.")
        logging.error(f"Non-retryable error for `{package}`: {stderr_text}")
        logging.debug(f"Command: `{' '.join(command)}`")
        logging.debug(f"Exit code: {process.returncode}")
        logging.debug(f"STDOUT: {stdout.decode().strip()}")
        logging.debug(f"STDERR: {stderr.decode().strip()}")

        deployment_results["failed"].append(package)

        return file, 1

    logging.info(f"Successfully deployed `{package}` to {environment} environment.")
    deployment_results["successful"].append(package)
    return file, 0


def create_summary_panel() -> Panel:
    """Create Rich panel with deployment summary."""
    successful = deployment_results["successful"]
    failed = deployment_results["failed"]

    table = Table(title="Deployment Summary", expand=True)

    table.add_column("Status", style="bold", ratio=1)
    table.add_column("Count", justify="right", ratio=1)
    table.add_column("Packages", ratio=4, overflow="ellipsis")

    table.add_row(
        "✅ Successful",
        str(len(successful)),
        ", ".join(successful) if successful else "None",
        style="green",
    )
    table.add_row(
        "❌ Failed", str(len(failed)), ", ".join(failed) if failed else "None", style="red"
    )

    total = len(successful) + len(failed)
    success_rate = (len(successful) / total * 100) if total > 0 else 0
    summary_text = (
        f"Total: {total} | Success Rate: {success_rate:.1f}%"
        if total > 0
        else "No deployments processed"
    )

    return Panel(table, title="🚀 Prefect Deployment Results", subtitle=summary_text)


def create_github_summary() -> None:
    """Create GitHub workflow summary from Rich output."""
    github_summary_path = environ.get("GITHUB_STEP_SUMMARY")
    if not github_summary_path:
        return

    with console.capture() as capture:
        console.print(create_summary_panel())

    _ = Path(github_summary_path).write_text(f"```\n{capture.get()}\n```")


def display_deployment_summary() -> None:
    """Display a Rich summary of deployment results."""
    console.print(create_summary_panel())
    create_github_summary()


async def main() -> None:
    """Main entrypoint for deploying all Prefect flows."""
    logging.info("Starting deployment of Prefect flows...")

    logging.debug(f"Environment: `{CONFIG.environment}`")
    logging.debug(f"Force deploy: `{CONFIG.force_deploy}`")
    logging.debug(f"SHA: `{CONFIG.sha}`")
    logging.debug(f"Batch size: `{CONFIG.batch_size}`")
    logging.debug(f"Docker cleanup: `{CONFIG.cleanup_docker}`")
    logging.debug(f"Max retries: `{CONFIG.max_retries}`")
    logging.debug(f"Deployment timeout: `{CONFIG.deployment_timeout}`")
    logging.debug(f"Retry delay: `{CONFIG.retry_delay}`")
    logging.debug(f"Backoff multiplier: `{CONFIG.backoff_multiplier}`")

    validate_configuration()

    pipelines = validate_pipelines_path(CONFIG.pipelines_path)
    yamls = await discover_yaml_files(pipelines, CONFIG.pipeline)

    logging.info(f"Found {len(yamls)} flow(s) to deploy: {[str(y) for y in yamls]}")

    failed_deployments = await execute_deployments(yamls)

    display_deployment_summary()

    if failed_deployments:
        errors = [str(e) for e in set(failed_deployments)]
        logging.error(
            f"Deployment completed with errors in {len(failed_deployments)} flow(s): {errors}"
        )
        sys.exit(1)


uvloop.run(main())
