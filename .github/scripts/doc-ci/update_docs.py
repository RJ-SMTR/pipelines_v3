"""
update_docs.py — Monta contexto e chama Claude (Sonnet) via Vertex AI para propor atualizações.

Uso:
    python scripts/doc-ci/update_docs.py \
        --diff generated/pipelines_v3/diff.json \
        --git-diff /tmp/git_diff.txt \
        --docs-dir pipelines-docs/docs/public \
        --doc-type public \
        --output /tmp/proposed_changes.json

Autenticação: Application Default Credentials (ADC) do GCP.
    - No CI: via google-github-actions/auth (SA key ou Workload Identity Federation)
    - Local: via gcloud auth application-default login

Requer: pip install 'anthropic[vertex]'

Estratégia de contexto:
    - Sem limites artificiais: todo o diff, git diff e docs existentes são enviados.
    - Se o contexto total exceder ~125k tokens (~500k chars), o script divide as
      mudanças estruturais em batches e faz múltiplas chamadas ao Claude,
      agregando as propostas no final.
"""

import argparse
import json
import math
import os
import sys
from pathlib import Path

try:
    from anthropic import AnthropicVertex
except ImportError:
    print("ERRO: pacote 'anthropic[vertex]' não encontrado.")
    print("Instale com: pip install 'anthropic[vertex]'")
    sys.exit(1)


# ~4 chars per token (conservative estimate for mixed pt-BR/code content)
CHARS_PER_TOKEN = 4
# Leave room for system prompt (~2k tokens) + response (max_tokens)
MAX_CONTEXT_CHARS = 500_000  # ~125k tokens, safe for 200k window


PROMPT_PUBLIC = """Você está atualizando a documentação pública de uma plataforma de dados de transporte público (SMTR - Rio de Janeiro).

Público-alvo:
- área estratégica de outros municípios
- pesquisadores
- órgãos de controle
- concessionárias
- equipes técnicas externas

Considere:
- mudanças estruturais no grafo (nodes/edges/processos adicionados ou removidos)
- mudanças no código (diff do git)
- conteúdo atual da documentação pública

Proponha as alterações necessárias para manter a documentação alinhada à arquitetura atual.

Priorize:
- visão geral clara
- explicação dos componentes
- relações entre sistemas
- consistência com o que já existe

Evite:
- detalhes operacionais internos
- troubleshooting
- infraestrutura interna
- comandos de manutenção

Tom: institucional, claro e tecnicamente sólido.

ESTRUTURA DE DIRETÓRIOS DO REPOSITÓRIO DE DOCUMENTAÇÃO:
O repositório usa MkDocs (Material theme). A documentação pública fica em docs/public/.

  docs/
    public/          ← VOCÊ EDITA AQUI (documentação pública)
      visao_geral.md
      componentes.md
      ...
    tech/            ← NÃO EDITE (outra visão editorial)
    pipelines/       ← docs legadas (NÃO EDITE)

REGRAS PARA file_path:
- Use caminhos relativos a docs/ (ex: "public/visao_geral.md", "public/componentes/captura.md")
- SEMPRE dentro de public/ — nunca em tech/ ou na raiz
- Use nomes em snake_case, em português
- Se precisar de subdiretórios, crie livremente (ex: "public/componentes/tratamento.md")

FORMATO DE RESPOSTA:
Responda EXCLUSIVAMENTE em JSON válido, sem markdown, sem backticks, com a seguinte estrutura:
{
  "changes": [
    {
      "action": "update" | "create",
      "file_path": "public/nome_do_arquivo.md",
      "reason": "motivo da alteração",
      "content": "conteúdo completo do arquivo (markdown)"
    }
  ],
  "summary": "resumo das alterações propostas"
}

Se nenhuma alteração for necessária, retorne:
{"changes": [], "summary": "Nenhuma alteração necessária."}
"""

PROMPT_TECH = """Você está atualizando a documentação técnica da arquitetura de uma plataforma de dados de transporte público (SMTR - Rio de Janeiro).

Público-alvo:
- engenharia de dados
- operação da plataforma
- revisores técnicos internos

Considere:
- mudanças estruturais no grafo (nodes/edges/processos adicionados ou removidos)
- mudanças no código (diff do git)
- documentação técnica existente

Proponha as alterações necessárias.

Priorize:
- dependências entre componentes
- impacto das mudanças
- coerência arquitetural
- precisão técnica

Tom: técnico, direto e objetivo.

ESTRUTURA DE DIRETÓRIOS DO REPOSITÓRIO DE DOCUMENTAÇÃO:
O repositório usa MkDocs (Material theme). A documentação técnica fica em docs/tech/.

  docs/
    public/          ← NÃO EDITE (outra visão editorial)
    tech/            ← VOCÊ EDITA AQUI (documentação técnica)
      arquitetura.md
      dependencias.md
      ...
    pipelines/       ← docs legadas (NÃO EDITE)

REGRAS PARA file_path:
- Use caminhos relativos a docs/ (ex: "tech/arquitetura.md", "tech/pipelines/captura.md")
- SEMPRE dentro de tech/ — nunca em public/ ou na raiz
- Use nomes em snake_case, em português
- Se precisar de subdiretórios, crie livremente (ex: "tech/fluxos/tratamento.md")

FORMATO DE RESPOSTA:
Responda EXCLUSIVAMENTE em JSON válido, sem markdown, sem backticks, com a seguinte estrutura:
{
  "changes": [
    {
      "action": "update" | "create",
      "file_path": "tech/nome_do_arquivo.md",
      "reason": "motivo da alteração",
      "content": "conteúdo completo do arquivo (markdown)"
    }
  ],
  "summary": "resumo das alterações propostas"
}

Se nenhuma alteração for necessária, retorne:
{"changes": [], "summary": "Nenhuma alteração necessária."}
"""


def load_diff(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_git_diff(path: Path) -> str:
    """Carrega o git diff completo."""
    if not path.exists():
        return "(git diff não disponível)"
    return path.read_text(encoding="utf-8", errors="replace")


def load_existing_docs(docs_dir: Path) -> str:
    """Carrega todos os arquivos de documentação existentes."""
    if not docs_dir.exists():
        return "(diretório de documentação não encontrado)"

    docs = []
    md_files = sorted(docs_dir.rglob("*.md"))

    for md_file in md_files:
        rel_path = md_file.relative_to(docs_dir)
        content = md_file.read_text(encoding="utf-8", errors="replace")
        docs.append(f"### {rel_path}\n\n{content}")

    if not docs:
        return "(nenhum arquivo .md encontrado)"

    return "\n\n---\n\n".join(docs)


def build_structural_section(diff: dict) -> str:
    """Monta a seção de detalhes das mudanças estruturais."""
    structural = []

    if diff["files"]["added"]:
        added_files = [f["file_path"] for f in diff["files"]["added"]]
        structural.append("### Arquivos adicionados\n" + "\n".join(f"- {f}" for f in added_files))

    if diff["files"]["removed"]:
        removed_files = [f["file_path"] for f in diff["files"]["removed"]]
        structural.append(
            "### Arquivos removidos\n" + "\n".join(f"- {f}" for f in removed_files)
        )

    if diff["nodes"]["added"]:
        structural.append(
            "### Symbols adicionados\n"
            + "\n".join(
                f"- `{n['name']}` ({n['type']}) em `{n['file_path']}`"
                for n in diff["nodes"]["added"]
            )
        )

    if diff["nodes"]["removed"]:
        structural.append(
            "### Symbols removidos\n"
            + "\n".join(
                f"- `{n['name']}` ({n['type']}) em `{n['file_path']}`"
                for n in diff["nodes"]["removed"]
            )
        )

    if diff["processes"]["added"]:
        structural.append(
            "### Processos adicionados\n"
            + "\n".join(
                f"- **{p['label']}** ({p['type']}, {p['step_count']} steps)"
                for p in diff["processes"]["added"]
            )
        )

    if diff["processes"]["removed"]:
        structural.append(
            "### Processos removidos\n"
            + "\n".join(
                f"- **{p['label']}** ({p['type']})" for p in diff["processes"]["removed"]
            )
        )

    if diff["processes"]["modified"]:
        structural.append(
            "### Processos modificados\n"
            + "\n".join(
                f"- **{p['label']}**: {p['before_step_count']} → {p['after_step_count']} steps"
                for p in diff["processes"]["modified"]
            )
        )

    if diff["module_edges"].get("added"):
        structural.append(
            "### Novas conexões entre módulos\n"
            + "\n".join(
                f"- {e['source_module']} → {e['target_module']} ({e['call_count']} chamadas)"
                for e in diff["module_edges"]["added"]
            )
        )

    return "\n\n".join(structural) if structural else ""


def build_context(diff: dict, git_diff: str, existing_docs: str) -> str:
    """Monta o contexto estruturado para enviar ao Claude."""
    sections = []

    # 1. Resumo das mudanças estruturais
    sections.append(f"## Resumo das Mudanças Estruturais\n\n{diff['summary']}")

    # 2. Detalhes estruturais
    structural = build_structural_section(diff)
    if structural:
        sections.append(f"## Detalhes das Mudanças Estruturais\n\n{structural}")

    # 3. Git diff
    sections.append(f"## Diff do Código\n\n```diff\n{git_diff}\n```")

    # 4. Documentação existente
    sections.append(f"## Documentação Existente\n\n{existing_docs}")

    return "\n\n---\n\n".join(sections)


def estimate_tokens(text: str) -> int:
    """Estima tokens a partir do tamanho do texto."""
    return len(text) // CHARS_PER_TOKEN


def split_diff_into_batches(diff: dict, n_batches: int) -> list[dict]:
    """
    Divide as mudanças estruturais do diff em N batches.
    Cada batch mantém o resumo e module_edges, mas divide files, nodes, edges e processes.
    """
    batches = []
    for i in range(n_batches):
        batch = {
            "summary": diff["summary"]
            + f"\n\n(Batch {i + 1}/{n_batches} das mudanças estruturais)",
            "has_changes": diff["has_changes"],
        }

        for key in ("files", "nodes", "edges"):
            section = diff[key]
            added = section.get("added", [])
            removed = section.get("removed", [])

            batch_added = added[i * len(added) // n_batches : (i + 1) * len(added) // n_batches]
            batch_removed = removed[
                i * len(removed) // n_batches : (i + 1) * len(removed) // n_batches
            ]

            batch[key] = {
                "added": batch_added,
                "removed": batch_removed,
                "added_count": len(batch_added),
                "removed_count": len(batch_removed),
            }

        # Processes: split added/removed, keep modified in all batches
        procs = diff["processes"]
        p_added = procs.get("added", [])
        p_removed = procs.get("removed", [])
        batch["processes"] = {
            "added": p_added[
                i * len(p_added) // n_batches : (i + 1) * len(p_added) // n_batches
            ],
            "removed": p_removed[
                i * len(p_removed) // n_batches : (i + 1) * len(p_removed) // n_batches
            ],
            "modified": procs.get("modified", []),
            "added_count": 0,
            "removed_count": 0,
            "modified_count": len(procs.get("modified", [])),
        }
        batch["processes"]["added_count"] = len(batch["processes"]["added"])
        batch["processes"]["removed_count"] = len(batch["processes"]["removed"])

        # Module edges: always send all (small)
        batch["module_edges"] = diff.get("module_edges", {})

        batches.append(batch)

    return batches


def call_claude(
    system_prompt: str,
    context: str,
    model: str = "claude-sonnet-4-5@20250514",
    project_id: str = "rj-smtr",
    region: str = "global",
) -> dict:
    """Chama Claude via Vertex AI e retorna as alterações propostas."""
    client = AnthropicVertex(project_id=project_id, region=region)

    context_tokens = estimate_tokens(context)
    print(f"Chamando {model} via Vertex AI (project={project_id}, region={region})...")
    print(f"  Contexto: ~{len(context):,} chars (~{context_tokens:,} tokens estimados)")

    message = client.messages.create(
        model=model,
        max_tokens=16000,
        system=system_prompt,
        messages=[{"role": "user", "content": context}],
    )

    response_text = ""
    for block in message.content:
        if block.type == "text":
            response_text += block.text

    print(f"  Tokens usados: {message.usage.input_tokens} input, {message.usage.output_tokens} output")

    # Parse JSON response
    try:
        cleaned = response_text.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.split("\n", 1)[1]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
            cleaned = cleaned.strip()

        return json.loads(cleaned)
    except json.JSONDecodeError as e:
        print(f"AVISO: resposta do Claude não é JSON válido: {e}")
        print(f"Resposta raw (primeiros 500 chars): {response_text[:500]}")
        return {"changes": [], "summary": f"Erro ao parsear resposta: {e}"}


def merge_proposals(proposals: list[dict]) -> dict:
    """Agrega múltiplas propostas (de batches) em uma única."""
    all_changes = []
    summaries = []

    # Track files to avoid duplicate updates across batches
    seen_files = set()

    for proposal in proposals:
        for change in proposal.get("changes", []):
            fp = change.get("file_path", "")
            if fp not in seen_files:
                all_changes.append(change)
                seen_files.add(fp)
            else:
                # Later batch wants to update same file — keep the later version
                all_changes = [c for c in all_changes if c.get("file_path") != fp]
                all_changes.append(change)

        summary = proposal.get("summary", "")
        if summary and summary != "Nenhuma alteração necessária.":
            summaries.append(summary)

    return {
        "changes": all_changes,
        "summary": " | ".join(summaries) if summaries else "Nenhuma alteração necessária.",
    }


def main():
    parser = argparse.ArgumentParser(description="Propõe atualizações de documentação via Claude")
    parser.add_argument("--diff", required=True, help="Arquivo diff.json")
    parser.add_argument("--git-diff", required=True, help="Arquivo com git diff")
    parser.add_argument("--docs-dir", required=True, help="Diretório da documentação existente")
    parser.add_argument(
        "--doc-type",
        required=True,
        choices=["public", "tech"],
        help="Tipo de documentação",
    )
    parser.add_argument("--output", required=True, help="Saída com propostas de alteração")
    parser.add_argument(
        "--model",
        default="claude-sonnet-4-5@20250514",
        help="Modelo Claude no Vertex AI",
    )
    parser.add_argument(
        "--project-id",
        default=os.environ.get("ANTHROPIC_VERTEX_PROJECT_ID", "rj-smtr"),
        help="GCP Project ID (default: rj-smtr)",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("CLOUD_ML_REGION", "global"),
        help="Vertex AI region (default: global)",
    )
    args = parser.parse_args()

    # Carregar inputs
    diff = load_diff(Path(args.diff))

    if not diff.get("has_changes", False):
        print("Nenhuma mudança estrutural detectada. Nada a fazer.")
        result = {"changes": [], "summary": "Sem mudanças estruturais."}
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        sys.exit(0)

    git_diff = load_git_diff(Path(args.git_diff))
    existing_docs = load_existing_docs(Path(args.docs_dir))
    system_prompt = PROMPT_PUBLIC if args.doc_type == "public" else PROMPT_TECH

    # Build full context to check size
    full_context = build_context(diff, git_diff, existing_docs)
    total_chars = len(full_context) + len(system_prompt)

    if total_chars <= MAX_CONTEXT_CHARS:
        # Single call — everything fits
        print(f"Contexto total: ~{total_chars:,} chars — chamada única.")
        result = call_claude(
            system_prompt,
            full_context,
            model=args.model,
            project_id=args.project_id,
            region=args.region,
        )
    else:
        # Context too large — split structural changes into batches
        # Fixed parts: git_diff + existing_docs + system_prompt
        fixed_chars = len(git_diff) + len(existing_docs) + len(system_prompt) + 2000  # overhead
        available_for_structural = MAX_CONTEXT_CHARS - fixed_chars

        # Estimate structural section size
        structural_chars = len(build_structural_section(diff)) + len(diff["summary"])
        n_batches = max(2, math.ceil(structural_chars / max(available_for_structural, 1)))

        print(
            f"Contexto total: ~{total_chars:,} chars (excede {MAX_CONTEXT_CHARS:,}) "
            f"— dividindo em {n_batches} batches."
        )

        batches = split_diff_into_batches(diff, n_batches)
        proposals = []

        for i, batch_diff in enumerate(batches):
            print(f"\n--- Batch {i + 1}/{n_batches} ---")
            batch_context = build_context(batch_diff, git_diff, existing_docs)
            proposal = call_claude(
                system_prompt,
                batch_context,
                model=args.model,
                project_id=args.project_id,
                region=args.region,
            )
            proposals.append(proposal)

        result = merge_proposals(proposals)
        print(f"\nPropostas agregadas de {n_batches} batches.")

    # Salvar resultado
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    n_changes = len(result.get("changes", []))
    print(f"\nResultado: {n_changes} alteração(ões) proposta(s)")
    print(f"Resumo: {result.get('summary', '(sem resumo)')}")
    print(f"Salvo em {args.output}")


if __name__ == "__main__":
    main()
