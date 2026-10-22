# -*- coding: utf-8 -*-
import asyncio
import os
from pathlib import Path

import yaml
from prefect.blocks.notifications import DiscordWebhook


async def deploy_block(block_spec: dict) -> None:
    block_type = block_spec["type"]
    name = block_spec["name"]

    if block_type == "discord-webhook":
        url_env = block_spec.get("url_env")
        url = os.environ.get(url_env) if url_env else block_spec.get("url")
        if not url:
            print(f"Skipping block {name}: env var {url_env} not set")
            return
        # URL format: https://discord.com/api/webhooks/{webhook_id}/{webhook_token}
        parts = url.rstrip("/").split("/")
        webhook_id = parts[-2]
        webhook_token = parts[-1]
        block = DiscordWebhook(webhook_id=webhook_id, webhook_token=webhook_token)
        await block.save(name=name, overwrite=True)
        print(f"Block {block_type}/{name} created/updated")
    else:
        print(f"Unknown block type: {block_type}, skipping")


async def main() -> None:
    blocks_file = Path(__file__).parent / "blocks.yaml"
    with blocks_file.open("r") as f:
        data = yaml.safe_load(f)

    for block_spec in data.get("blocks", []):
        await deploy_block(block_spec)


if __name__ == "__main__":
    print("Starting blocks deployment...")
    asyncio.run(main())
