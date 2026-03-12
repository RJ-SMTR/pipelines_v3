"""
apply_changes.py — Aplica as alterações propostas pelo Claude nos arquivos de documentação.

Uso:
    python scripts/apply_changes.py \
        --proposals /tmp/proposed_public.json /tmp/proposed_tech.json \
        --docs-dir docs/
"""

import argparse
import json
import sys
from pathlib import Path


def apply_proposals(proposals_files: list[str], docs_dir: Path) -> list[dict]:
    """Aplica todas as propostas de alteração nos arquivos de docs."""
    applied = []

    for proposals_path in proposals_files:
        with open(proposals_path, encoding="utf-8") as f:
            proposals = json.load(f)

        for change in proposals.get("changes", []):
            action = change.get("action", "update")
            file_path = change.get("file_path", "")
            content = change.get("content", "")
            reason = change.get("reason", "")

            if not file_path or not content:
                print(f"  SKIP: proposta inválida (sem file_path ou content)")
                continue

            target = docs_dir / file_path
            target.parent.mkdir(parents=True, exist_ok=True)

            existed = target.exists()

            with open(target, "w", encoding="utf-8") as f:
                f.write(content)

            status = "updated" if existed else "created"
            print(f"  {status.upper()}: {file_path} — {reason}")

            applied.append(
                {
                    "action": status,
                    "file_path": file_path,
                    "reason": reason,
                }
            )

    return applied


def main():
    parser = argparse.ArgumentParser(description="Aplica propostas de alteração na documentação")
    parser.add_argument(
        "--proposals",
        nargs="+",
        required=True,
        help="Arquivo(s) JSON com propostas",
    )
    parser.add_argument("--docs-dir", required=True, help="Diretório raiz da documentação")
    args = parser.parse_args()

    docs_dir = Path(args.docs_dir)
    if not docs_dir.exists():
        print(f"Criando diretório de documentação: {docs_dir}")
        docs_dir.mkdir(parents=True)

    print(f"Aplicando propostas em {docs_dir}...")
    applied = apply_proposals(args.proposals, docs_dir)

    if not applied:
        print("Nenhuma alteração aplicada — documentação já está atualizada.")
        sys.exit(0)

    print(f"\n{len(applied)} arquivo(s) alterado(s).")
    sys.exit(0)


if __name__ == "__main__":
    main()
