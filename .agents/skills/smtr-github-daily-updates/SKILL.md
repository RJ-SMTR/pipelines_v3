---
name: smtr-github-daily-updates
description: Review commits from a user-selected period, infer which RJ-SMTR/pipelines_v3 subissues they belong to, identify Expedite work that interrupted planned activity, collect work completed without commits, and draft daily Shape Up progress comments for explicit approval before posting. Use when the user asks to report today's, yesterday's, or another period's development progress on GitHub subissues, prepare daily updates, map commits to active scope or incident issues, document Expedites, or catch up on missed issue comments.
---

# SMTR GitHub Daily Updates

## Overview

Prepare concise daily progress comments for implementation subissues in
`RJ-SMTR/pipelines_v3`. Infer commit-to-subissue associations from repository
evidence, but never publish comments or edit issue scope without explicit user
approval.

## Defaults

- Resolve the repository from the current checkout's `origin` remote. Fall back
  to `RJ-SMTR/pipelines_v3` only when it cannot be resolved.
- Use Project `RJ-SMTR/projects/21` and status `In progress` to identify work in
  development.
- Analyze commits authored by the current Git `user.name` or `user.email`.
  Include other authors only when the user requests it.
- Interpret dates in `America/Sao_Paulo` unless the user specifies another
  timezone.
- Convert GitHub UTC timestamps to the selected timezone before assigning
  activity to a calendar day. Do not use the UTC date as the comment date.
- Treat the selected period as inclusive. For a single day, analyze from
  `00:00:00` through `23:59:59` in the selected timezone.
- Generate one independent comment per issue per calendar day. A catch-up
  period never produces one consolidated multi-day comment.
- Comment only on subissues with relevant work in the period. Do not add empty
  daily comments.
- Treat Project 21 items with `Raia=🔥 Expedite` as interruptive work. Include
  Expedites opened, worked on, or closed during the period even when their
  current status is already `Done`.
- Include planned issues completed during the selected period. Current
  `Status=Done` does not mean the issue was never interrupted before completion.

## Required Workflow

### 1. Establish the period and scope

1. Ask which period to analyze unless the user already supplied one. Suggest
   the current local date as the default and accept ranges such as yesterday
   through today for missed updates.
2. State the exact inclusive dates before analyzing. Do not rely only on words
   such as `today` or `yesterday`.
3. Ask whether any calendar dates inside the selected period must be excluded,
   unless the user already specified exclusions. Remove excluded dates before
   collecting and drafting activity.
4. If the user provides a root issue, limit candidates to its subissues.
5. Otherwise, find issues from the current repository that are in Project 21
   with `Status=In progress`, planned issues completed during the period, and
   Expedites opened, worked on, or closed during the period. Prefer issues
   assigned to the authenticated GitHub user. If this produces an ambiguous or
   very broad set, show the candidates and ask the user to narrow the scope.

### 2. Read active issue context

For each candidate issue, read:

- number, title, body, URL, state, assignees, labels, and issue type;
- Project 21 fields, especially `Status`;
- parent issue and sibling subissues when available;
- recent comments covering or immediately preceding the selected period, to
  avoid repeating work already reported.

Use `gh issue view` for basic issue data and GitHub GraphQL for subissue
relationships and Project v2 fields. Keep open implementation subissues with
`Status=In progress` as normal planned-work targets. Also keep planned issues
closed or marked `Done` during the period, and issues with `Raia=🔥 Expedite`
when their incident or implementation activity overlaps the selected period.

Do not rely only on the current Project status. Use issue close timestamps,
merged PR timestamps, commit chronology, Project field history when available,
and recent comments to reconstruct whether an issue was active earlier in the
period.

### 3. Identify Expedites and interruptions

Classify an issue as an Expedite when Project 21 has `Raia=🔥 Expedite`.
Incident or hotfix wording is supporting evidence, but must not override an
explicit non-Expedite Project value.

For each Expedite:

1. identify the incident, impact, corrective action, PRs, commits, and current
   outcome;
2. build a chronological activity timeline around the Expedite using commit
   authored times, PR activity, branch changes, issue events, and comments;
3. identify planned issues with activity immediately before the Expedite,
   including issues later resumed and completed during the same period;
4. look for evidence of resumption after the Expedite, such as later commits,
   a merged PR, issue closure, or a `Done` transition;
5. treat timing as candidate evidence, not proof: require the user to confirm
   which planned issue was actually interrupted;
6. ask the user which planned subissue was interrupted when this cannot be
   narrowed to one strong candidate from the timeline;
7. include the Expedite's own update and also record the interruption in the
   affected planned subissue's comment.

Do not describe an Expedite as planned scope. Keep incident response visibly
separate from normal cycle progress.

### 4. Collect commits

1. Read the current Git identity:

   ```bash
   git config user.name
   git config user.email
   ```

2. List authored commits in the inclusive period with full commit messages,
   timestamps, changed file names, and stats. Include all relevant local
   branches and remote-tracking refs when available, not only the currently
   checked-out branch.
3. De-duplicate commits by full SHA.
4. Analyze merge and integration events separately from authored implementation
   commits. A merge of older work may still represent review, requested
   changes, CI monitoring, conflict resolution, deployment coordination, or a
   context switch performed during the selected day.
5. Do not describe old implementation as newly developed on the merge date.
   Record only the review or integration activity supported by PR reviews,
   same-day commits, CI events, comments, merge-queue activity, or user
   confirmation.
6. Treat a merge or review as an interruption only when chronology and context
   support it, then ask the user to confirm the interrupted issue. Do not infer
   interruption from the merge timestamp alone.
7. Exclude mechanical merge commits when they add no meaningful same-day work
   beyond activity already represented by the PR timeline.
8. Do not assume the commit subject is enough. Inspect the diff or changed
   files whenever needed for classification.

### 5. Infer the corresponding subissue

Score each commit against candidate subissues using, in descending order:

1. explicit issue number or URL in the commit message, branch, PR metadata, or
   related GitHub event;
2. exact identifiers shared by the diff and issue body, including model, table,
   pipeline, flow, selector, dataset, macro, and file names;
3. semantic match between the implemented behavior and the issue objective,
   solution, and implementation checklist;
4. repository area and overlap with files previously associated with the
   subissue;
5. parent and sibling scope boundaries, using them to reject a superficially
   similar but incorrect subissue.

Classify confidence as:

- `high`: direct reference or multiple strong independent matches;
- `medium`: one strong semantic or file-level match with no competing issue;
- `low`: plausible match with incomplete evidence or competing candidates;
- `unresolved`: no defensible match.

Automatically draft under `high` and `medium` associations, but show the
evidence and confidence. Ask the user to confirm `low` associations. Never
assign an `unresolved` commit merely to ensure every commit appears somewhere.

For work without commits, also allow an operational-dependency association.
Use it when the activity is necessary to implement, validate, diagnose, or
operate the subissue even if its wording and artifacts do not match the issue
body directly. Examples include validating upstream GPS API behavior for a
travel-validation issue, materializing prerequisite tables, checking external
responses, or coordinating access. Explain the dependency and ask for
confirmation when it is not obvious.

### 6. Handle commits outside the documented scope

For every `low` or `unresolved` commit, present:

- short SHA and subject;
- concise description of the actual change;
- changed files or identifiers that matter;
- closest candidate subissues, if any;
- why the existing scope does not clearly cover it.

Ask the user to choose whether the work:

- belongs to an existing subissue and should be included in its comment;
- requires updating that subissue's title, body, or checklist;
- belongs only in the root issue's high-level scope;
- represents a new scope that may require a new subissue;
- should be excluded from this daily update.

Do not edit an issue or create a subissue as part of inference. First draft the
exact proposed scope update separately and request explicit approval. When a
new subissue is needed, use the `smtr-github-subissues` skill rather than
creating it ad hoc.

### 7. Collect work without commits

After showing the inferred mapping, explicitly ask whether the user performed
additional relevant work without a commit during the period. Present the
mapped subissues by number and title so the user can attach each activity to
the correct issue.

Examples include investigation, validation, deployment, data checks,
stakeholder alignment, manual configuration, and blocked work. Distinguish
completed work from next steps and blockers.

Before declaring an activity unresolved, test whether it has an operational
dependency on an existing subissue. Prefer recording it in that issue when the
user confirms the dependency; do not require a new subissue merely because the
activity lacks a commit or direct textual match.

Also ask whether an Expedite interrupted a planned activity and, if so, which
subissue was interrupted and whether any relevant context-switch cost or
post-incident follow-up should be recorded. When the candidate issue was later
completed, ask for confirmation using the full sequence:

> O Expedite #NNN parece ter interrompido a issue #MMM, que foi retomada e
> concluída depois. Essa sequência está correta?

### 8. Draft the comments

For planned subissues, use:

```markdown
## Atualização — DD/MM/YYYY

### Realizado
- Descrição objetiva da entrega ([`abc1234`](commit-url))
- Atividade relevante realizada sem commit

### Expedites / Interrupções
- A atividade foi interrompida para tratar a issue [#NNN](issue-url): descrição curta do incidente e resultado. Em seguida, foi retomada e concluída.

### Próximos passos
- Próximo passo informado pelo usuário
```

Omit `Expedites / Interrupções` when the planned work was not interrupted.
Adapt the final sentence to the observed outcome: `retomada e concluída`,
`retomada e ainda em andamento`, or `ainda não retomada`.

For an Expedite issue, use:

```markdown
## Atualização — DD/MM/YYYY

### Expedite
- **Incidente:** descrição objetiva do problema e impacto.
- **Ação:** correção ou mitigação realizada ([PR #NNN](pr-url); [`abc1234`](commit-url)).
- **Resultado:** estado verificado após a intervenção.

### Trabalho interrompido
- [#NNN](issue-url) — atividade planejada pausada para tratar este incidente, retomada e concluída posteriormente.

### Pendências
- Acompanhamento ou correção definitiva, quando aplicável.
```

Omit `Trabalho interrompido` when no planned subissue was displaced. Omit
`Pendências` when the incident is fully resolved and no follow-up was
identified.

For a multi-day catch-up, create separate comments headed
`## Atualização — DD/MM/YYYY` for each day with relevant activity. Never use a
date range in one comment.

Formatting rules:

- Omit `Próximos passos` when none were supplied or clearly established.
- Add `### Bloqueios` only when blockers exist.
- Group several commits into one outcome-oriented bullet when they implement
  the same change.
- Mention commit links with short SHAs, but describe delivered behavior rather
  than copying commit subjects mechanically.
- Do not claim deployment, validation, or completion unless supported by the
  commits or confirmed by the user.
- For an Expedite, state why it interrupted the cycle and what result restored
  service or reduced impact. Do not hide it inside a generic `Realizado`
  bullet.
- Write in Portuguese unless the user asks for another language.
- Keep the update concise and useful to someone following the Shape Up cycle.

### 9. Require approval before publication

Show:

1. the analyzed period, excluded dates, timezone, and author filter;
2. commit-to-subissue mapping with confidence;
3. Expedites and the planned subissues they interrupted;
4. review and integration work distinguished from implementation authored that
   day;
5. unresolved commits and any proposed issue-scope edits;
6. the exact final comment for every target issue and calendar day.

Ask for explicit authorization to publish. Accept approval for all comments or
for selected issue numbers. Apply requested revisions and show the changed
comment again before posting when the change is material.

Post approved comments with `gh issue comment`. Never post an unapproved
comment. Never treat authorization to comment as authorization to edit issue
bodies, titles, Project fields, status, progress, or checklists.

### 10. Verify and report

After posting:

1. read each created comment back from GitHub;
2. report the issue number, title, comment URL, and covered period;
3. report comments that were skipped or remained unresolved;
4. compare the published work with the target subissue and root issue scopes;
5. report scope drift when the work reveals:
   - completed checklist outcomes that remain unchecked;
   - an implementation approach different from the documented architecture;
   - terminology or business-rule inconsistencies between comments and issue bodies;
   - work that belongs in an existing issue but is absent from its objective;
6. propose concise title, objective, or checklist updates separately and require explicit
   approval before editing;
7. separately report any proposed issue updates that still await approval.

When proposing checklist changes, record meaningful deliverables, validations, and remaining
risks. Do not add obvious investigation or implementation steps such as identifying the flow,
locating a file, or configuring a helper when those steps have no independent outcome value.

## Safety

- Perform all discovery and drafting read-only.
- Require separate explicit approvals for comments, issue edits, and new
  subissues.
- Do not expose unrelated commits, secrets, generated credentials, or sensitive
  diff contents in comments.
- Do not duplicate an existing update for the same work and period.
- If GitHub history and local Git history disagree, state the limitation and
  ask before omitting potentially relevant work.
