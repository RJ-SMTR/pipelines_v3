# -*- coding: utf-8 -*-
"""
extract_graph.py — Extrai graph.json do KuzuDB gerado pelo GitNexus.

Uso:
    python scripts/extract_graph.py \
        --kuzu-path .gitnexus \
        --repo-name pipelines_rj_smtr \
        --output generated/repo-a/graph.json

Requer: pip install kuzu
"""

import argparse
import json
import subprocess
import sys
from pathlib import Path

try:
    import kuzu
except ImportError:
    print("ERRO: pacote 'kuzu' não encontrado. Instale com: pip install kuzu")
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


def extract_nodes(conn: kuzu.Connection) -> list[dict]:
    """Extrai todos os symbols definidos em arquivos."""
    # KuzuDB requires explicit node types — query each DEFINES target separately
    symbol_types = ["Function", "Class", "Method", "CodeElement"]
    nodes = []
    for stype in symbol_types:
        try:
            rows = conn.execute(
                f"""
                MATCH (f:File)-[r:CodeRelation]->(n:{stype})
                WHERE r.type = 'DEFINES'
                RETURN f.filePath AS file_path,
                       n.name AS name,
                       n.isExported AS is_exported
                ORDER BY file_path, name
                """
            ).get_as_df()
            for _, row in rows.iterrows():
                nodes.append(
                    {
                        "id": f"{row['file_path']}::{row['name']}",
                        "file_path": row["file_path"],
                        "name": row["name"],
                        "type": stype,
                        "is_exported": bool(row["is_exported"]),
                    }
                )
        except Exception:
            continue
    return nodes


def extract_files(conn: kuzu.Connection) -> list[dict]:
    """Extrai todos os arquivos rastreados no grafo."""
    rows = conn.execute(
        """
        MATCH (f:File)
        RETURN f.filePath AS file_path
        ORDER BY file_path
        """
    ).get_as_df()

    return [{"file_path": row["file_path"]} for _, row in rows.iterrows()]


def extract_edges(conn: kuzu.Connection) -> list[dict]:
    """Extrai todas as chamadas entre arquivos (inter-file calls)."""
    rows = conn.execute(
        """
        MATCH (a:Function)-[r:CodeRelation]->(b:Function)
        WHERE r.type = 'CALLS' AND a.filePath <> b.filePath
        RETURN DISTINCT
            a.filePath AS source_file,
            a.name AS source_name,
            b.filePath AS target_file,
            b.name AS target_name
        ORDER BY source_file, target_file
        """
    ).get_as_df()

    edges = []
    for _, row in rows.iterrows():
        edges.append(
            {
                "source": f"{row['source_file']}::{row['source_name']}",
                "target": f"{row['target_file']}::{row['target_name']}",
                "type": "CALLS",
            }
        )
    return edges


def extract_imports(conn: kuzu.Connection) -> list[dict]:
    """Extrai edges de import entre arquivos."""
    try:
        rows = conn.execute(
            """
            MATCH (a:File)-[r:CodeRelation]->(b:File)
            WHERE r.type = 'IMPORTS'
            RETURN DISTINCT
                a.filePath AS source_file,
                b.filePath AS target_file
            ORDER BY source_file, target_file
            """
        ).get_as_df()

        return [
            {
                "source": row["source_file"],
                "target": row["target_file"],
                "type": "IMPORTS",
            }
            for _, row in rows.iterrows()
        ]
    except Exception:
        # IMPORTS edge pode não existir no schema
        return []


def extract_processes(conn: kuzu.Connection) -> list[dict]:
    """Extrai fluxos de execução (processes) detectados pelo GitNexus."""
    proc_rows = conn.execute(
        """
        MATCH (p:Process)
        RETURN p.id AS id,
               p.heuristicLabel AS label,
               p.processType AS type,
               p.stepCount AS step_count
        ORDER BY step_count DESC
        """
    ).get_as_df()

    processes = []
    for _, proc in proc_rows.iterrows():
        proc_id = proc["id"]
        safe_id = str(proc_id).replace("'", "''")

        step_rows = conn.execute(
            f"""
            MATCH (s:Function)-[r:CodeRelation]->(p:Process)
            WHERE r.type = 'STEP_IN_PROCESS' AND p.id = '{safe_id}'
            RETURN s.name AS name,
                   s.filePath AS file_path,
                   r.step AS step_order
            ORDER BY r.step
            """
        ).get_as_df()

        steps = []
        for _, step in step_rows.iterrows():
            steps.append(
                {
                    "step": int(step["step_order"]) if step["step_order"] else 0,
                    "name": step["name"],
                    "file_path": step["file_path"],
                    "type": "Function",
                }
            )

        processes.append(
            {
                "id": proc_id,
                "label": proc["label"] or proc_id,
                "type": proc["type"] or "unknown",
                "step_count": int(proc["step_count"]) if proc["step_count"] else 0,
                "steps": steps,
            }
        )

    return processes


def extract_module_edges(conn: kuzu.Connection, files: list[dict]) -> list[dict]:
    """
    Agrupa chamadas inter-file por módulo (diretório de primeiro nível).
    Equivalente ao getInterModuleEdgesForOverview() do GitNexus wiki.
    """
    # Mapear arquivo → módulo (primeiro diretório)
    file_to_module = {}
    for f in files:
        parts = f["file_path"].split("/")
        module = parts[0] if len(parts) > 1 else "__root__"
        file_to_module[f["file_path"]] = module

    rows = conn.execute(
        """
        MATCH (a:Function)-[r:CodeRelation]->(b:Function)
        WHERE r.type = 'CALLS' AND a.filePath <> b.filePath
        RETURN DISTINCT a.filePath AS source_file, b.filePath AS target_file
        """
    ).get_as_df()

    module_counts: dict[str, int] = {}
    for _, row in rows.iterrows():
        src_mod = file_to_module.get(row["source_file"])
        tgt_mod = file_to_module.get(row["target_file"])
        if src_mod and tgt_mod and src_mod != tgt_mod:
            key = f"{src_mod}|||{tgt_mod}"
            module_counts[key] = module_counts.get(key, 0) + 1

    module_edges = []
    for key, count in sorted(module_counts.items(), key=lambda x: -x[1]):
        src, tgt = key.split("|||")
        module_edges.append({"source_module": src, "target_module": tgt, "call_count": count})

    return module_edges


def main():
    parser = argparse.ArgumentParser(description="Extrai graph.json do KuzuDB (GitNexus)")
    parser.add_argument("--kuzu-path", default=".gitnexus", help="Caminho do banco KuzuDB")
    parser.add_argument("--repo-name", required=True, help="Nome do repositório")
    parser.add_argument("--output", required=True, help="Caminho de saída do graph.json")
    args = parser.parse_args()

    kuzu_path = Path(args.kuzu_path)
    # GitNexus stores the database as a single file at <dir>/kuzu
    if kuzu_path.is_dir():
        db_file = kuzu_path / "kuzu"
        if db_file.exists():
            kuzu_path = db_file
        else:
            print(f"ERRO: banco KuzuDB não encontrado em {kuzu_path}")
            print("Execute 'gitnexus analyze' primeiro.")
            sys.exit(1)
    elif not kuzu_path.exists():
        print(f"ERRO: banco KuzuDB não encontrado em {kuzu_path}")
        print("Execute 'gitnexus analyze' primeiro.")
        sys.exit(1)

    print(f"Conectando ao KuzuDB em {kuzu_path}...")
    db = kuzu.Database(str(kuzu_path))
    conn = kuzu.Connection(db)

    print("Extraindo arquivos...")
    files = extract_files(conn)
    print(f"  → {len(files)} arquivos")

    print("Extraindo nodes (symbols)...")
    nodes = extract_nodes(conn)
    print(f"  → {len(nodes)} symbols")

    print("Extraindo edges (calls)...")
    edges = extract_edges(conn)
    print(f"  → {len(edges)} chamadas inter-file")

    print("Extraindo imports...")
    imports = extract_imports(conn)
    print(f"  → {len(imports)} imports")

    print("Extraindo processes...")
    processes = extract_processes(conn)
    print(f"  → {len(processes)} fluxos de execução")

    print("Calculando edges entre módulos...")
    module_edges = extract_module_edges(conn, files)
    print(f"  → {len(module_edges)} conexões inter-módulo")

    graph = {
        "repository": args.repo_name,
        "commit_sha": get_commit_sha(),
        "files": files,
        "nodes": nodes,
        "edges": edges + imports,
        "processes": processes,
        "module_edges": module_edges,
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(graph, f, indent=2, ensure_ascii=False, sort_keys=True)

    print(f"\ngraph.json salvo em {output_path}")
    print(f"  Arquivos: {len(files)}")
    print(f"  Symbols: {len(nodes)}")
    print(f"  Edges: {len(edges) + len(imports)}")
    print(f"  Processes: {len(processes)}")


if __name__ == "__main__":
    main()
