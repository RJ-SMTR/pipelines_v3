# -*- coding: utf-8 -*-
"""
compare_dbt_graphs.py — Compara dois dbt_graph.json e produz um diff estrutural.

Uso:
    python scripts/doc-ci/compare_dbt_graphs.py \
        --before generated/pipelines_v3/dbt_graph.json \
        --after /tmp/dbt_graph_new.json \
        --output /tmp/dbt_diff.json

Retorna exit code 0 se houve mudanças, 1 se idênticos.
"""

import argparse
import json
import sys
from pathlib import Path


def load_graph(path: Path) -> dict:
    if not path.exists():
        return {"models": [], "sources": [], "edges": [], "selectors": []}
    with path.open(encoding="utf-8") as f:
        return json.load(f)


def diff_models(before: list[dict], after: list[dict]) -> dict:
    """Compara modelos por unique_id, detectando adições, remoções e modificações."""
    before_by_id = {m["unique_id"]: m for m in before}
    after_by_id = {m["unique_id"]: m for m in after}

    before_ids = set(before_by_id.keys())
    after_ids = set(after_by_id.keys())

    added = [after_by_id[k] for k in sorted(after_ids - before_ids)]
    removed = [before_by_id[k] for k in sorted(before_ids - after_ids)]

    # Detectar modificações em modelos existentes
    modified = []
    for mid in sorted(before_ids & after_ids):
        old = before_by_id[mid]
        new = after_by_id[mid]
        changes = []

        if sorted(old.get("depends_on", [])) != sorted(new.get("depends_on", [])):
            changes.append("depends_on")
        if old.get("materialized") != new.get("materialized"):
            changes.append("materialized")
        if old.get("schema") != new.get("schema"):
            changes.append("schema")
        if old.get("description") != new.get("description"):
            changes.append("description")

        # Comparar colunas
        old_cols = {c["name"] for c in old.get("columns", [])}
        new_cols = {c["name"] for c in new.get("columns", [])}
        if old_cols != new_cols:
            changes.append("columns")

        if changes:
            modified.append(
                {
                    "unique_id": mid,
                    "name": new.get("name", ""),
                    "changed_fields": changes,
                }
            )

    return {
        "added": added,
        "removed": removed,
        "modified": modified,
        "added_count": len(added),
        "removed_count": len(removed),
        "modified_count": len(modified),
    }


def diff_sources(before: list[dict], after: list[dict]) -> dict:
    """Compara sources por unique_id."""
    before_keys = {s["unique_id"] for s in before}
    after_keys = {s["unique_id"] for s in after}

    after_lookup = {s["unique_id"]: s for s in after}
    before_lookup = {s["unique_id"]: s for s in before}

    added_keys = after_keys - before_keys
    removed_keys = before_keys - after_keys

    return {
        "added": [after_lookup[k] for k in sorted(added_keys)],
        "removed": [before_lookup[k] for k in sorted(removed_keys)],
        "added_count": len(added_keys),
        "removed_count": len(removed_keys),
    }


def diff_edges(before: list[dict], after: list[dict]) -> dict:
    """Compara edges usando source+target como chave composta."""

    def edge_key(e: dict) -> str:
        return f"{e['source']}→{e['target']}"

    before_keys = {edge_key(e) for e in before}
    after_keys = {edge_key(e) for e in after}

    after_lookup = {edge_key(e): e for e in after}
    before_lookup = {edge_key(e): e for e in before}

    added_keys = after_keys - before_keys
    removed_keys = before_keys - after_keys

    return {
        "added": [after_lookup[k] for k in sorted(added_keys)],
        "removed": [before_lookup[k] for k in sorted(removed_keys)],
        "added_count": len(added_keys),
        "removed_count": len(removed_keys),
    }


def diff_selectors(before: list[dict], after: list[dict]) -> dict:
    """Compara selectors por nome."""
    before_by_name = {s["name"]: s for s in before}
    after_by_name = {s["name"]: s for s in after}

    before_names = set(before_by_name.keys())
    after_names = set(after_by_name.keys())

    added = [after_by_name[k] for k in sorted(after_names - before_names)]
    removed = [before_by_name[k] for k in sorted(before_names - after_names)]

    modified = []
    for name in sorted(before_names & after_names):
        old = json.dumps(before_by_name[name], sort_keys=True)
        new = json.dumps(after_by_name[name], sort_keys=True)
        if old != new:
            modified.append(after_by_name[name])

    return {
        "added": added,
        "removed": removed,
        "modified": modified,
        "added_count": len(added),
        "removed_count": len(removed),
        "modified_count": len(modified),
    }


def summarize(diff: dict) -> str:
    """Gera um resumo legível do diff."""
    parts = []

    models = diff["models"]
    if models["added_count"] or models["removed_count"] or models["modified_count"]:
        parts.append(
            f"Modelos: +{models['added_count']} -{models['removed_count']}"
            f" ~{models['modified_count']}"
        )

    sources = diff["sources"]
    if sources["added_count"] or sources["removed_count"]:
        parts.append(f"Sources: +{sources['added_count']} -{sources['removed_count']}")

    edges = diff["edges"]
    if edges["added_count"] or edges["removed_count"]:
        parts.append(f"Edges: +{edges['added_count']} -{edges['removed_count']}")

    selectors = diff["selectors"]
    if selectors["added_count"] or selectors["removed_count"] or selectors["modified_count"]:
        parts.append(
            f"Selectors: +{selectors['added_count']} -{selectors['removed_count']}"
            f" ~{selectors['modified_count']}"
        )

    if not parts:
        return "Nenhuma mudança no DAG dbt."

    return "Mudanças no DAG dbt: " + " | ".join(parts)


def main():
    parser = argparse.ArgumentParser(description="Compara dois dbt_graph.json")
    parser.add_argument("--before", required=True, help="dbt_graph.json anterior")
    parser.add_argument("--after", required=True, help="dbt_graph.json novo")
    parser.add_argument("--output", required=True, help="Saída do dbt_diff.json")
    args = parser.parse_args()

    before = load_graph(Path(args.before))
    after = load_graph(Path(args.after))

    diff = {
        "repository": after.get("repository", before.get("repository", "unknown")),
        "before_commit": before.get("commit_sha", "unknown"),
        "after_commit": after.get("commit_sha", "unknown"),
        "models": diff_models(before.get("models", []), after.get("models", [])),
        "sources": diff_sources(before.get("sources", []), after.get("sources", [])),
        "edges": diff_edges(before.get("edges", []), after.get("edges", [])),
        "selectors": diff_selectors(before.get("selectors", []), after.get("selectors", [])),
    }

    has_changes = any(
        [
            diff["models"]["added_count"],
            diff["models"]["removed_count"],
            diff["models"]["modified_count"],
            diff["sources"]["added_count"],
            diff["sources"]["removed_count"],
            diff["edges"]["added_count"],
            diff["edges"]["removed_count"],
            diff["selectors"]["added_count"],
            diff["selectors"]["removed_count"],
            diff["selectors"]["modified_count"],
        ]
    )

    diff["has_changes"] = has_changes
    diff["summary"] = summarize(diff)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        json.dump(diff, f, indent=2, ensure_ascii=False)

    print(diff["summary"])

    # Exit code: 0 = mudanças detectadas, 1 = sem mudanças
    sys.exit(0 if has_changes else 1)


if __name__ == "__main__":
    main()
