# -*- coding: utf-8 -*-
import argparse
import asyncio

import yaml
from prefect.automations import Automation


async def upsert_automation(auto_spec):
    automation = Automation(**auto_spec)
    print(f"Processing automation: {automation.name}")
    try:
        existing = await automation.read(name=automation.name)
    except ValueError:
        existing = None

    if existing:
        print(f"Updating existing automation: {automation.name}")
        print(f"Existing automation details: {existing}")
        automation.id = existing.id
        await automation.update()
    else:
        print(f"Creating new automation: {automation.name}")
        await automation.create()


async def main(yaml_file):
    with open(yaml_file, "r") as f:
        data = yaml.safe_load(f)
        print("Loaded YAML data: ", data)

    automations = data.get("automations", [])
    for auto_spec in automations:
        await upsert_automation(auto_spec)


if __name__ == "__main__":
    print("Starting automation deployment...")
    parser = argparse.ArgumentParser(description="Create Prefect automations from YAML file.")
    parser.add_argument(
        "yaml_file", help="Path to the YAML file containing automation definitions."
    )
    args = parser.parse_args()
    asyncio.run(main(args.yaml_file))
