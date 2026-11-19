#!/bin/sh
set -eu

openmetadata_version=1.13.3.0
# Keep dependency resolution within the last successful GTFS staging build.
dependency_cutoff=2026-11-18T19:41:14Z
venv_dir=/opt/openmetadata

export UV_PYTHON_INSTALL_DIR=/opt/openmetadata-python
UV_CACHE_DIR=$(mktemp -d /tmp/openmetadata-uv-cache.XXXXXX)
export UV_CACHE_DIR
trap 'rm -rf "$UV_CACHE_DIR"' EXIT

# OpenMetadata uses its own Python 3.11 environment.
uv python install 3.11
uv venv --python 3.11 "$venv_dir"
uv --no-config pip install --no-cache \
    --exclude-newer "$dependency_cutoff" \
    --python "$venv_dir/bin/python" \
    "openmetadata-ingestion[dbt]==$openmetadata_version"

# Verify that the CLI starts even with the flow's Python 3.13 settings.
PYTHONHOME=/usr/local PYTHONPATH=/usr/local/lib/python3.13 \
    "$venv_dir/bin/python" -I "$venv_dir/bin/metadata" --version
