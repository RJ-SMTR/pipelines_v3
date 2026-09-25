# -*- coding: utf-8 -*-
"""Select changed dbt model files and expose the generation flag to GitHub Actions."""

import argparse
import json
import os
import subprocess
from pathlib import Path


def main(output_path: Path) -> None:
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    changed = []
    if "pull_request" in event:
        pull = event["pull_request"]
        head = pull["head"]["sha"]
        before = event.get("before") if event.get("action") == "synchronize" else None
        if before:
            if subprocess.run(
                ["git", "cat-file", "-e", before], capture_output=True, check=False
            ).returncode:
                subprocess.run(["git", "fetch", "--no-tags", "origin", before], check=True)
        else:
            before = subprocess.check_output(
                ["git", "merge-base", pull["base"]["sha"], head], text=True
            ).strip()
        changed = (
            subprocess.check_output(
                ["git", "diff", "--name-only", "--no-renames", "-z", before, head]
            )
            .decode()
            .split("\0")
        )
    models = [
        path
        for path in changed
        if path.startswith("queries/models/")
        and (Path(path).match("base_*.sql") or Path(path).suffix in {".yml", ".yaml"})
    ]
    output_path.write_text(json.dumps(models))
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.write(f"generate={str(bool(models)).lower()}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("/tmp/changed-contract-models.json"))
    main(parser.parse_args().output)
