# -*- coding: utf-8 -*-
import argparse
import asyncio
from pathlib import Path

import yaml
from prefect.automations import Automation
from prefect.client.orchestration import get_client


async def resolve_block_refs(spec: dict) -> dict:
    action_fields = ["actions", "actions_on_trigger", "actions_on_resolve"]

    refs = {
        action["block_document_id"]
        for field in action_fields
        for action in spec.get(field, [])
        if isinstance(action.get("block_document_id"), str)
        and action["block_document_id"].startswith("$block:")
    }
    if not refs:
        return spec

    ref_to_uuid: dict = {}
    async with get_client() as client:
        for ref in refs:
            _, path = ref.split(":", 1)
            block_type, block_name = path.split("/", 1)
            block_doc = await client.read_block_document_by_name(
                block_name, block_type, include_secrets=False
            )
            ref_to_uuid[ref] = str(block_doc.id)

    for field in action_fields:
        for action in spec.get(field, []):
            if action.get("block_document_id") in ref_to_uuid:
                action["block_document_id"] = ref_to_uuid[action["block_document_id"]]

    return spec


async def upsert_automation(auto_spec: dict) -> None:
    auto_spec = await resolve_block_refs(auto_spec)
    automation = Automation(**auto_spec)
    print(f"Processing automation: {automation.name}")
    try:
        existing = await automation.read(name=automation.name)
    except ValueError:
        existing = None

    if existing:
        print(f"Updating existing automation: {automation.name}")
        print(f"  {existing.name}\n  {existing.id}")
        automation.id = existing.id
        await automation.update()
    else:
        print(f"Creating new automation: {automation.name}")
        await automation.create()


async def main(yaml_file: str) -> None:
    with Path.open(yaml_file, "r") as f:
        data = yaml.safe_load(f)

    for auto_spec in data.get("automations", []):
        await upsert_automation(auto_spec)


if __name__ == "__main__":
    print("Starting automation deployment...")
    parser = argparse.ArgumentParser(description="Create Prefect automations from YAML file.")
    parser.add_argument(
        "yaml_file", help="Path to the YAML file containing automation definitions."
    )
    args = parser.parse_args()
    asyncio.run(main(args.yaml_file))
