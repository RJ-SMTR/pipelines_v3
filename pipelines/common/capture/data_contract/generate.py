# -*- coding: utf-8 -*-
"""Generate checked-in ODCS contracts from capture source settings and a dbt manifest."""

from __future__ import annotations

import argparse
import importlib
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import yaml
from open_data_contract_standard.model import OpenDataContractStandard

from pipelines.common.capture.data_contract.dbt_importer import (
    import_contract_from_manifest,
)
from pipelines.common.capture.data_contract.repository import contract_relative_path
from pipelines.common.capture.data_contract.utils import adapt_contract_schema
from pipelines.common.utils.gcp.bigquery import SourceTable

ROOT = Path(__file__).resolve().parents[4]
DEFAULT_MANIFEST = ROOT / "queries" / "target" / "manifest.json"


def _capture_sources() -> list[SourceTable]:
    """Import capture constants and discover their configured SourceTable objects.

    Import failures intentionally propagate: silently skipping a source module could
    make the generator report a false clean result.
    """
    sources = []
    for constants_path in sorted((ROOT / "pipelines").glob("capture__*/constants.py")):
        module_name = ".".join(constants_path.relative_to(ROOT).with_suffix("").parts)
        module = importlib.import_module(module_name)
        for value in vars(module).values():
            candidates = (value,) if isinstance(value, SourceTable) else value
            if isinstance(candidates, (list, tuple)):
                sources.extend(source for source in candidates if isinstance(source, SourceTable))
    return sources


def _managed_contracts(
    sources: Iterable[Any],
) -> dict[Path, tuple[str, tuple[str, ...], tuple[str, ...]]]:
    contracts: dict[Path, tuple[str, tuple[str, ...], tuple[str, ...]]] = {}
    for source in sources:
        if not getattr(source, "validate_data_contract", False):
            continue
        model_name = getattr(source, "data_contract_model", None)
        source_name = getattr(source, "source_name", None)
        if not source_name or not model_name:
            raise ValueError(
                "Enabled data contract sources need source_name and data_contract_model"
            )
        relative_path = Path(contract_relative_path(source_name, model_name)).relative_to(
            "contracts"
        )
        settings = (
            model_name,
            tuple(getattr(source, "data_contract_ignored_columns", ()) or ()),
            tuple(getattr(source, "primary_keys", ()) or ()),
        )
        previous = contracts.setdefault(relative_path, settings)
        if previous != settings:
            raise ValueError(
                f"Sources mapped to contracts/{relative_path} have inconsistent model, "
                "ignored columns, or primary keys"
            )
    return contracts


def _render_contract(
    *,
    manifest: Path,
    model_name: str,
    ignored_columns: tuple[str, ...],
    primary_keys: tuple[str, ...],
    source_name: str,
) -> str:
    contract = adapt_contract_schema(
        import_contract_from_manifest(manifest, model_name),
        ignored_columns=ignored_columns,
        primary_keys=primary_keys,
    )
    contract["id"] = f"urn:datacontract:{source_name}:{model_name}"
    contract["name"] = f"{source_name}/{model_name}"
    OpenDataContractStandard.model_validate(contract)
    return yaml.safe_dump(contract, sort_keys=False, allow_unicode=True)


def generate_contracts(
    *,
    manifest_path: Path = DEFAULT_MANIFEST,
    contracts_dir: Path = ROOT / "contracts",
    check: bool = False,
) -> list[str]:
    """Generate contracts, or check the same output directory for drift."""
    if not manifest_path.is_file():
        raise FileNotFoundError(f"dbt manifest not found at {manifest_path}; run dbt parse first")

    managed = _managed_contracts(_capture_sources())
    messages = []
    for path, (model, ignored_columns, primary_keys) in sorted(managed.items()):
        expected = _render_contract(
            manifest=manifest_path,
            model_name=model,
            ignored_columns=ignored_columns,
            primary_keys=primary_keys,
            source_name=path.parts[0],
        )
        output = contracts_dir / path
        if check:
            if not output.is_file():
                messages.append(f"missing {output}")
            elif output.read_text(encoding="utf-8") != expected:
                messages.append(f"out of date {output}")
        else:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(expected, encoding="utf-8")
            messages.append(f"generated {output}")

    if check:
        messages.extend(
            f"unregistered contract {path}"
            for path in sorted(contracts_dir.rglob("*.odcs.yaml"))
            if path.relative_to(contracts_dir) not in managed
        )
        if messages:
            raise ValueError("Contract drift detected:\n" + "\n".join(messages))
        return ["checked-in contracts match dbt and source configuration"]
    return messages


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--contracts-dir", type=Path, default=ROOT / "contracts")
    parser.add_argument("--check", action="store_true", help="fail if committed ODCS files drift")
    args = parser.parse_args()
    for message in generate_contracts(
        manifest_path=args.manifest,
        contracts_dir=args.contracts_dir,
        check=args.check,
    ):
        print(message)


if __name__ == "__main__":
    main()
