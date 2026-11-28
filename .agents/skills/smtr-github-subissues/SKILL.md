---
name: smtr-github-subissues
description: Create and maintain RJ-SMTR GitHub subissues from a root issue using the team's issue template, link them to the parent issue, add them to Project 21, fill Project fields, and keep existing child and root scopes synchronized. Use when the user asks to split a root issue, create cards, update an existing subissue's title/body/checklist, reconcile completed work with documented scope, or inspect tactical board fields.
---

# SMTR GitHub Subissues

## Overview

Use this skill to create or maintain implementation subissues in `RJ-SMTR/pipelines_v3`.
Prefer updating an existing matching subissue over creating a parallel card, and keep the root
issue as the concise high-level view of the current scope.

Default behavior:

- Repository: resolve from the current checkout's `origin` remote. Fall back to `RJ-SMTR/pipelines_v3` only when the repo cannot be resolved.
- Project owner: `RJ-SMTR`
- Project number: `21`
- Tactical view: `8` (`Visão Tática`)
- Issue template: `.github/ISSUE_TEMPLATE/escopo.md` / GitHub template name `escopo`
- Issue type: `🔨 Escopo`
- Default fields: `Status=To Do`, `Apetite=⏱️ 2 semanas`, and
  `Raia=🗂️ Small Batch`.
- `Núcleo`: inherit from the root issue's Project field. Fall back to `Inovação` only when the root issue has no `Núcleo` value in Project 21.
- Do not fill `Progresso` unless the user explicitly asks.

## Required Workflow

1. Resolve the current repository first:

   ```bash
   gh repo view --json nameWithOwner -q .nameWithOwner
   ```

   If that fails, parse `git remote get-url origin`. Use a user-provided repo only when the user explicitly names one.
2. Read the root issue with `gh issue view <number> --repo <repo> --json number,title,body,state,url`.
3. Read existing subissues before proposing new ones:

   ```bash
   gh api graphql -f query='query($owner:String!, $repo:String!, $number:Int!) { repository(owner:$owner, name:$repo) { issue(number:$number) { number title url subIssues(first:50) { totalCount nodes { number title url state body } } } } }' -F owner=<owner> -F repo=<name> -F number=<number>
   ```

4. Inspect the current repo and reuse existing names and patterns whenever they clarify the issue:

   - Use `rg`/`rg --files` to find mentioned tables, dbt models, macros, Prefect flows, Discord/webhook helpers, and existing calendar/Sheets logic.
   - Prefer real local identifiers in checklist items, such as model names, paths, selectors, flow names, table names, macros, and existing scripts.
   - Keep the issue body readable for the board; do not dump implementation research into the issue if it does not help execution.
5. Identify subissue candidates from the root issue, usually from sections like `A Solução`, bold subsection headings, bullets, and explicit architecture chunks.
6. Avoid duplicates by comparing candidates against existing subissue titles and bodies. If a matching subissue exists, reuse it and report that it already exists instead of recreating it.
7. Read and use the repo's current `escopo` template as the source of truth:

   ```bash
   sed -n '1,120p' .github/ISSUE_TEMPLATE/escopo.md
   ```

   If the local template is missing, fetch the GitHub template with:

   ```bash
   gh api graphql -f query='query($owner:String!, $repo:String!) { repository(owner:$owner, name:$repo) { issueTemplates { name body filename } } }' -F owner=<owner> -F repo=<name>
   ```

8. Generate each issue body by filling the `escopo` template sections. Preserve the section titles and comments from the template:

   ```markdown
   ## 🎯 Contexto e Origem

   **Objetivo deste Escopo:** ...

   ...

   ## 🕵️‍♂️ Discovered Tasks (Checklist de Implementação)

   <!-- NÃO preencha isso tudo no dia 1. Adicione as tarefas reais (dbt, Prefect, BigQuery) à medida que você esbarra nelas durante a execução. O Escopo deve cruzar do ingest até o data warehouse. -->

   - [ ] ...

   ## ⚖️ Nice-to-Haves (Candidatos ao Scope Hammering)

   <!-- Tarefas que melhorariam o código/entrega, mas NÃO são impeditivas para a funcionalidade principal. Se o tempo apertar, elas serão sumariamente cortadas. Marque com um til (~). -->

   - [ ] ~...~
   ```

9. Keep task lists outcome-oriented and concise:
   - include meaningful deliveries, validations, production transitions, and unresolved risks;
   - combine tightly coupled implementation steps into one outcome;
   - omit obvious discovery or mechanics such as identifying a flow, locating a file, reading
     existing code, wiring a helper, or configuring a secret when they have no independent
     delivery value;
   - put optional polish, documentation, dashboards, extra tests, and operational enhancements
     in `Nice-to-Haves`.
10. Create a JSON plan file with this shape:

   ```json
   {
     "issues": [
       {
         "title": "Titulo do subissue",
         "body": "## 🎯 Contexto e Origem\n...",
         "status": "To Do",
         "apetite": "⏱️ 2 semanas",
         "raia": "🗂️ Small Batch"
       }
     ]
   }
   ```

   Omit `nucleo` unless the user explicitly wants to override the inherited parent value. Omit `progresso` unless the user asks for it. Omit `assignees` unless the user explicitly names assignees; the script-level assignee mode controls the default. If fields are omitted, the script defaults to `Status=To Do`, `Apetite=⏱️ 2 semanas`, `Raia=🗂️ Small Batch`, and inherited `Núcleo`. The script accepts a unique emoji-free alias such as `2 semanas`, but applies and reports the canonical Project option.

11. Run the bundled script once with `--dry-run` and review the output:

   ```bash
   python .agents/skills/smtr-github-subissues/scripts/create_subissues.py --parent <issue-number> --plan <plan.json> --dry-run
   ```

   Pass `--repo <owner/name>` only when the user explicitly asks to target a different repository.
12. Before creating live issues, ask the user which assignee behavior to use:

   - `parent-fallback-viewer`: assign the same users as the root issue; if the root issue has no assignee, assign the authenticated GitHub user.
   - `blank`: create subissues without assignees.

   The script also supports `parent` and `viewer`, but do not choose those unless the user asks.

13. If the user asked to actually create issues, run the same command without `--dry-run` and include the chosen assignee mode:

   ```bash
   python .agents/skills/smtr-github-subissues/scripts/create_subissues.py --parent <issue-number> --plan <plan.json> --assignee-mode parent-fallback-viewer
   ```

14. Report created issue numbers/URLs, reused existing subissues, issue type, assignee mode, inherited `Núcleo`, and the Project field values applied.

## Maintaining Existing Scope

When the request concerns work already represented by a subissue:

1. read the child issue, root issue, recent progress comments, related PRs/commits, and relevant
   Project fields;
2. compare the documented objective, architecture, terminology, and checklist with the delivered
   behavior;
3. update the existing child instead of creating a new issue when its objective still owns the
   work;
4. update the root issue only at overview level when the architecture, business rule, or scope
   boundary changed materially;
5. preserve useful unchecked outcomes and remove or consolidate obsolete, duplicate, or obvious
   checklist steps;
6. review whether the title still represents the resulting scope;
7. show the exact proposed child and root edits and require explicit approval before applying
   them;
8. apply only the approved title/body changes; do not infer approval for Project fields, status,
   progress, or assignees;
9. read every edited issue back from GitHub and verify its final title, objective, and checklist.

## GitHub Project Fields

The script discovers field and option IDs from Project 21 at runtime. Expected field names and options:

- `Status`: `Pitches em Estruturação`, `To Do`, `In progress`, `In review`, `Done`
- `Apetite`: `🗓️ 6 semanas`, `⏱️ 2 semanas`, `⚡ SLA Urgência`
- `Raia`: `📦 Big Batch`, `🗂️ Small Batch`, `🔥 Expedite`
- `Núcleo`: `Bilhetagem`, `Subsídio`, `Inovação`, `Governança`, `Analytics`, `Infraestrutura`
- `Progresso`: `⛰️ Uphill`, `🏃 Downhill`

Inherit `Núcleo` from the root issue by default, falling back to `Inovação` only when the root issue has no `Núcleo` value in Project 21. Do not fill `Progresso` by default.

## Authentication Requirements

Use `gh` for GitHub operations. The token needs access to repo issues and GitHub Projects v2:

- `repo`
- `read:project`
- `project`

If Project calls fail with `INSUFFICIENT_SCOPES`, ask the user to run:

```bash
gh auth refresh -h github.com -s read:project -s project
```

## Safety

Never create live issues before showing or running a dry-run plan unless the user explicitly asks to skip review. If a root issue already has subissues that match the generated candidates, do not recreate them.
