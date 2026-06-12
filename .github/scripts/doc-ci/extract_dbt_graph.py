# -*- coding: utf-8 -*-
"""
extract_dbt_graph.py — Extrai dbt_graph.json do manifest.json gerado por `dbt parse`.

Uso:
    python scripts/doc-ci/extract_dbt_graph.py \
        --manifest queries/target/manifest.json \
        --selectors queries/selectors.yml \
        --output /tmp/dbt_graph.json

Requer: pyyaml (incluso no dbt)
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    print("ERRO: pacote 'pyyaml' não encontrado. Instale com: pip install pyyaml")
    sys.exit(1)


def get_commit_sha() -> str:
    """Obtém o SHA do commit atual via git."""
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def extract_models(manifest: dict) -> list[dict]:
    """Extrai modelos do manifest, filtrando por resource_type == 'model'."""
    models = []
    for _node_id, node in manifest.get("nodes", {}).items():
        if node.get("resource_type") != "model":
            continue

        columns = []
        for col_name, col_info in node.get("columns", {}).items():
            columns.append(
                {
                    "name": col_name,
                    "type": col_info.get("data_type", ""),
                    "description": col_info.get("description", ""),
                }
            )

        models.append(
            {
                "unique_id": node["unique_id"],
                "name": node.get("name", ""),
                "path": node.get("path", ""),
                "schema": node.get("schema", ""),
                "materialized": node.get("config", {}).get("materialized", ""),
                "description": node.get("description", ""),
                "depends_on": node.get("depends_on", {}).get("nodes", []),
                "tags": node.get("tags", []),
                "columns": columns,
            }
        )

    return sorted(models, key=lambda m: m["unique_id"])


def extract_sources(manifest: dict) -> list[dict]:
    """Extrai sources do manifest."""
    sources = []
    for _source_id, source in manifest.get("sources", {}).items():
        sources.append(
            {
                "unique_id": source["unique_id"],
                "name": source.get("name", ""),
                "schema": source.get("schema", ""),
                "database": source.get("database", ""),
                "description": source.get("description", ""),
            }
        )

    return sorted(sources, key=lambda s: s["unique_id"])


def build_edges(models: list[dict]) -> list[dict]:
    """Deriva edges a partir do depends_on de cada modelo."""
    edges = []
    for model in models:
        for dep in model["depends_on"]:
            edge_type = "ref" if dep.startswith("model.") else "source"
            edges.append(
                {
                    "source": model["unique_id"],
                    "target": dep,
                    "type": edge_type,
                }
            )

    return sorted(edges, key=lambda e: (e["source"], e["target"]))


def extract_selectors(selectors_path: Path) -> list[dict]:
    """Extrai selectors do selectors.yml."""
    if not selectors_path.exists():
        return []

    with selectors_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not data or "selectors" not in data:
        return []

    selectors = []
    for sel in data["selectors"]:
        selectors.append(
            {
                "name": sel.get("name", ""),
                "description": sel.get("description", ""),
            }
        )

    return sorted(selectors, key=lambda s: s["name"])


def main():
    parser = argparse.ArgumentParser(description="Extrai dbt_graph.json do manifest.json")
    parser.add_argument("--manifest", required=True, help="Caminho do manifest.json")
    parser.add_argument("--selectors", default=None, help="Caminho do selectors.yml")
    parser.add_argument("--output", required=True, help="Caminho de saída do dbt_graph.json")
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    if not manifest_path.exists():
        print(f"ERRO: manifest.json não encontrado em {manifest_path}")
        print("Execute 'dbt parse --no-partial-parse' primeiro.")
        sys.exit(1)

    print(f"Lendo manifest.json de {manifest_path}...")
    with manifest_path.open(encoding="utf-8") as f:
        manifest = json.load(f)

    print("Extraindo modelos...")
    models = extract_models(manifest)
    print(f"  → {len(models)} modelos")

    print("Extraindo sources...")
    sources = extract_sources(manifest)
    print(f"  → {len(sources)} sources")

    print("Construindo edges...")
    edges = build_edges(models)
    print(f"  → {len(edges)} edges")

    selectors = []
    if args.selectors:
        print("Extraindo selectors...")
        selectors = extract_selectors(Path(args.selectors))
        print(f"  → {len(selectors)} selectors")

    graph = {
        "repository": "pipelines_v3",
        "commit_sha": get_commit_sha(),
        "models": models,
        "sources": sources,
        "edges": edges,
        "selectors": selectors,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(graph, f, indent=2, ensure_ascii=False)

    print(f"\ndbt_graph.json salvo em {output_path}")
    print(f"  Modelos: {len(models)}")
    print(f"  Sources: {len(sources)}")
    print(f"  Edges: {len(edges)}")
    print(f"  Selectors: {len(selectors)}")


if __name__ == "__main__":
    main()
