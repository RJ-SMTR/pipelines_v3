"""
compare_graphs.py — Compara dois graph.json e produz um diff estrutural.

Uso:
    python scripts/compare_graphs.py \
        --before generated/repo-a/graph.json \
        --after generated/repo-a/graph_new.json \
        --output generated/repo-a/diff.json

Retorna exit code 0 se houve mudanças, 1 se idênticos.
"""

import argparse
import json
import sys
from pathlib import Path


def load_graph(path: Path) -> dict:
    if not path.exists():
        return {"files": [], "nodes": [], "edges": [], "processes": [], "module_edges": []}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def diff_list_by_key(before: list[dict], after: list[dict], key: str) -> dict:
    """Compara duas listas de dicts por uma chave, retornando added/removed."""
    before_keys = {item[key] for item in before}
    after_keys = {item[key] for item in after}

    added_keys = after_keys - before_keys
    removed_keys = before_keys - after_keys

    after_lookup = {item[key]: item for item in after}
    before_lookup = {item[key]: item for item in before}

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


def diff_processes(before: list[dict], after: list[dict]) -> dict:
    """Compara processes por id, detectando adições, remoções e modificações."""
    before_by_id = {p["id"]: p for p in before}
    after_by_id = {p["id"]: p for p in after}

    before_ids = set(before_by_id.keys())
    after_ids = set(after_by_id.keys())

    added = [after_by_id[k] for k in sorted(after_ids - before_ids)]
    removed = [before_by_id[k] for k in sorted(before_ids - after_ids)]

    # Detectar processes que mudaram (steps diferentes)
    modified = []
    for pid in sorted(before_ids & after_ids):
        old_steps = json.dumps(before_by_id[pid].get("steps", []), sort_keys=True)
        new_steps = json.dumps(after_by_id[pid].get("steps", []), sort_keys=True)
        if old_steps != new_steps:
            modified.append(
                {
                    "id": pid,
                    "label": after_by_id[pid].get("label", pid),
                    "before_step_count": before_by_id[pid].get("step_count", 0),
                    "after_step_count": after_by_id[pid].get("step_count", 0),
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


def diff_module_edges(before: list[dict], after: list[dict]) -> dict:
    """Compara conexões inter-módulo."""

    def me_key(e: dict) -> str:
        return f"{e['source_module']}→{e['target_module']}"

    before_map = {me_key(e): e for e in before}
    after_map = {me_key(e): e for e in after}

    before_keys = set(before_map.keys())
    after_keys = set(after_map.keys())

    added = [after_map[k] for k in sorted(after_keys - before_keys)]
    removed = [before_map[k] for k in sorted(before_keys - after_keys)]

    # Conexões que mudaram de peso (call_count)
    changed = []
    for key in sorted(before_keys & after_keys):
        old_count = before_map[key]["call_count"]
        new_count = after_map[key]["call_count"]
        if old_count != new_count:
            changed.append(
                {
                    "source_module": after_map[key]["source_module"],
                    "target_module": after_map[key]["target_module"],
                    "before_count": old_count,
                    "after_count": new_count,
                    "delta": new_count - old_count,
                }
            )

    return {
        "added": added,
        "removed": removed,
        "changed": changed,
    }


def summarize(diff: dict) -> str:
    """Gera um resumo legível do diff."""
    lines = []

    files = diff["files"]
    if files["added_count"] or files["removed_count"]:
        lines.append(f"Arquivos: +{files['added_count']} -{files['removed_count']}")

    nodes = diff["nodes"]
    if nodes["added_count"] or nodes["removed_count"]:
        lines.append(f"Symbols: +{nodes['added_count']} -{nodes['removed_count']}")

    edges = diff["edges"]
    if edges["added_count"] or edges["removed_count"]:
        lines.append(f"Edges: +{edges['added_count']} -{edges['removed_count']}")

    procs = diff["processes"]
    if procs["added_count"] or procs["removed_count"] or procs["modified_count"]:
        lines.append(
            f"Processes: +{procs['added_count']} -{procs['removed_count']} ~{procs['modified_count']}"
        )

    me = diff["module_edges"]
    if me["added"] or me["removed"] or me["changed"]:
        lines.append(
            f"Conexões inter-módulo: +{len(me['added'])} -{len(me['removed'])} ~{len(me['changed'])}"
        )

    if not lines:
        return "Nenhuma mudança estrutural detectada."

    return "Mudanças estruturais detectadas:\n" + "\n".join(f"  • {l}" for l in lines)


def main():
    parser = argparse.ArgumentParser(description="Compara dois graph.json")
    parser.add_argument("--before", required=True, help="graph.json anterior")
    parser.add_argument("--after", required=True, help="graph.json novo")
    parser.add_argument("--output", required=True, help="Saída do diff.json")
    args = parser.parse_args()

    before = load_graph(Path(args.before))
    after = load_graph(Path(args.after))

    diff = {
        "repository": after.get("repository", before.get("repository", "unknown")),
        "before_commit": before.get("commit_sha", "unknown"),
        "after_commit": after.get("commit_sha", "unknown"),
        "files": diff_list_by_key(before.get("files", []), after.get("files", []), "file_path"),
        "nodes": diff_list_by_key(before.get("nodes", []), after.get("nodes", []), "id"),
        "edges": diff_edges(before.get("edges", []), after.get("edges", [])),
        "processes": diff_processes(before.get("processes", []), after.get("processes", [])),
        "module_edges": diff_module_edges(
            before.get("module_edges", []), after.get("module_edges", [])
        ),
    }

    # Verificar se houve mudanças
    has_changes = any(
        [
            diff["files"]["added_count"],
            diff["files"]["removed_count"],
            diff["nodes"]["added_count"],
            diff["nodes"]["removed_count"],
            diff["edges"]["added_count"],
            diff["edges"]["removed_count"],
            diff["processes"]["added_count"],
            diff["processes"]["removed_count"],
            diff["processes"]["modified_count"],
            diff["module_edges"]["added"],
            diff["module_edges"]["removed"],
            diff["module_edges"]["changed"],
        ]
    )

    diff["has_changes"] = has_changes
    diff["summary"] = summarize(diff)

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(diff, f, indent=2, ensure_ascii=False)

    print(diff["summary"])

    # Exit code: 0 = mudanças detectadas, 1 = sem mudanças
    sys.exit(0 if has_changes else 1)


if __name__ == "__main__":
    main()
