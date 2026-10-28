# -*- coding: utf-8 -*-
"""Resolve the PyPI package version and Dataproc image tag for GitHub Actions."""

import hashlib
import json
import os
import re
import urllib.request
from pathlib import Path

IMAGE_INPUTS = (
    "dataproc.Dockerfile",
    "requirements-dataproc.txt",
    ".github/workflows/build-and-push-dataproc-image.yaml",
    ".github/scripts/resolve_dataproc_image.py",
)


def main():
    url = "https://pypi.org/pypi/rio-rac-bus-subsidy/json"
    with urllib.request.urlopen(url, timeout=30) as response:
        release = json.load(response)
    version = release["info"]["version"]
    if not re.fullmatch(r"[0-9][A-Za-z0-9.!+_-]{0,100}", version):
        raise ValueError("Invalid PyPI version")
    if not any(not file.get("yanked", False) for file in release["urls"]):
        raise ValueError("The latest release has no non-yanked distributions")

    fingerprint = hashlib.sha256(version.encode())
    for name in IMAGE_INPUTS:
        fingerprint.update(name.encode())
        fingerprint.update(Path(name).read_bytes())
    tag = "build-" + fingerprint.hexdigest()[:24]
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.write(f"version={version}\ntag={tag}\n")
    print(f"Package: rio-rac-bus-subsidy=={version}; image tag: {tag}")


if __name__ == "__main__":
    main()
