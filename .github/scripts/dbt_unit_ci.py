# -*- coding: utf-8 -*-
"""Prepare and clean the isolated BigQuery datasets where dbt runs the tested models' unit tests."""

import argparse
import json
import os
import re
from pathlib import Path

from google.cloud import bigquery

PROJECT_ID = "rj-smtr-dev"
TARGET_PATH = Path("queries/target")
PLAN_PATH = TARGET_PATH / "unit-test-plan.json"


def validate_schema(schema: str, prefix: str) -> None:
    if not re.fullmatch(r"unit_pr_\d+_\d+_\d+", prefix):
        raise ValueError("DBT_USER must identify a unique PR, run and attempt")
    if not schema.startswith(f"{prefix}__") or not re.fullmatch(r"\w+", schema):
        raise ValueError(f"Dataset is outside this CI run: {schema}")


def create_plan(manifest: dict, prefix: str) -> dict:
    unit_tests = manifest.get("unit_tests", {})
    nodes = manifest["nodes"]
    tested_ids = {
        uid
        for test in unit_tests.values()
        for uid in test["depends_on"]["nodes"]
        if uid.startswith("model.")
    }
    schemas = set()
    for uid in tested_ids:
        node = nodes[uid]
        if node["database"] != PROJECT_ID:
            raise ValueError(f"Model would write outside the development project: {uid}")
        validate_schema(node["schema"], prefix)
        schemas.add(node["schema"])
    return {
        "project": PROJECT_ID,
        "prefix": prefix,
        "schemas": sorted(schemas),
        "unit_tests": sorted(unit_tests),
    }


def prepare() -> None:
    manifest = json.loads((TARGET_PATH / "manifest.json").read_text())
    plan = create_plan(manifest, os.environ["DBT_USER"])
    # Persist the complete cleanup scope before any remote writes.
    PLAN_PATH.write_text(json.dumps(plan, indent=2) + "\n")
    client = bigquery.Client(project=PROJECT_ID)
    for schema in plan["schemas"]:
        dataset = bigquery.Dataset(f"{PROJECT_ID}.{schema}")
        dataset.location = "US"
        dataset.default_table_expiration_ms = 24 * 60 * 60 * 1000
        client.create_dataset(dataset, exists_ok=True, timeout=60)
    print(f"Prepared {len(plan['schemas'])} datasets for {len(plan['unit_tests'])} unit tests")


def cleanup() -> None:
    if not PLAN_PATH.exists():
        print("No unit-test dataset plan exists; no cleanup needed")
        return
    plan = json.loads(PLAN_PATH.read_text())
    prefix = os.environ["DBT_USER"]
    if plan["project"] != PROJECT_ID or plan["prefix"] != prefix:
        raise ValueError("Cleanup plan does not belong to this CI run")
    for schema in plan["schemas"]:
        validate_schema(schema, prefix)
    client = bigquery.Client(project=PROJECT_ID)
    for schema in plan["schemas"]:
        client.delete_dataset(
            f"{PROJECT_ID}.{schema}", delete_contents=True, not_found_ok=True, timeout=60
        )
    print(f"Deleted {len(plan['schemas'])} isolated unit-test datasets")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("prepare", "cleanup"))
    args = parser.parse_args()
    if args.action == "prepare":
        prepare()
    else:
        cleanup()
