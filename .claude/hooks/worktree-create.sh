#!/bin/bash
# WorktreeCreate hook — creates worktree with staging/<name> branch
# Input (stdin JSON): { "name": "<slug>", "cwd": "<project-root>" }
# Output (stdout): absolute path to the created worktree

set -e

INPUT=$(cat)
NAME=$(echo "$INPUT" | jq -r '.name')
CWD=$(echo "$INPUT" | jq -r '.cwd')

GIT_ROOT="${CWD:-$(git rev-parse --show-toplevel)}"
WORKTREE_DIR="$GIT_ROOT/.claude/worktrees"
WORKTREE_PATH="$WORKTREE_DIR/$NAME"
BRANCH_NAME="staging/$NAME"

# Ensure .claude/worktrees is in .gitignore
if ! grep -qF '.claude/worktrees' "$GIT_ROOT/.gitignore" 2>/dev/null; then
  [[ -f "$GIT_ROOT/.gitignore" && -n "$(tail -c 1 "$GIT_ROOT/.gitignore")" ]] && echo "" >> "$GIT_ROOT/.gitignore"
  echo ".claude/worktrees" >> "$GIT_ROOT/.gitignore"
  echo "Added .claude/worktrees to .gitignore" >&2
fi

# Determine base branch
DEFAULT_BRANCH=$(git symbolic-ref refs/remotes/origin/HEAD 2>/dev/null | sed 's@^refs/remotes/origin/@@') || true
if [[ -z "$DEFAULT_BRANCH" ]]; then
  for candidate in dev main master; do
    if git show-ref --verify --quiet "refs/remotes/origin/$candidate" 2>/dev/null; then
      DEFAULT_BRANCH="$candidate"
      break
    fi
  done
fi
DEFAULT_BRANCH="${DEFAULT_BRANCH:-dev}"

# Create worktree
mkdir -p "$WORKTREE_DIR"

if git show-ref --verify --quiet "refs/heads/$BRANCH_NAME" 2>/dev/null; then
  echo "Using existing branch: $BRANCH_NAME" >&2
  git worktree add "$WORKTREE_PATH" "$BRANCH_NAME" >&2
else
  echo "Creating branch: $BRANCH_NAME (from origin/$DEFAULT_BRANCH)" >&2
  git worktree add -b "$BRANCH_NAME" "$WORKTREE_PATH" "origin/$DEFAULT_BRANCH" >&2
  # Fix upstream: track own remote branch instead of base branch
  if git show-ref --verify --quiet "refs/remotes/origin/$BRANCH_NAME" 2>/dev/null; then
    git -C "$WORKTREE_PATH" branch --set-upstream-to="origin/$BRANCH_NAME" "$BRANCH_NAME" >&2
  else
    git -C "$WORKTREE_PATH" branch --unset-upstream "$BRANCH_NAME" 2>/dev/null || true
  fi
fi

# Copy .worktreeinclude files
INCLUDE_FILE="$GIT_ROOT/.worktreeinclude"

if [[ -f "$INCLUDE_FILE" ]]; then
  file_list=$(git -C "$GIT_ROOT" ls-files --others --ignored --exclude-from="$INCLUDE_FILE" 2>/dev/null)

  if [[ -z "$file_list" ]]; then
    echo "No files matched .worktreeinclude patterns" >&2
  else
    count=$(echo "$file_list" | wc -l | tr -d ' ')
    echo "Copying $count file(s) from .worktreeinclude..." >&2

    git -C "$GIT_ROOT" ls-files -z --others --ignored --exclude-from="$INCLUDE_FILE" 2>/dev/null | \
      tar -C "$GIT_ROOT" --null -T - -cf - 2>/dev/null | \
      tar -C "$WORKTREE_PATH" -xf - 2>/dev/null

    echo "$file_list" | awk -F/ '{print ($2 ? $1"/" : $0)}' | sort | uniq -c | \
      while read -r cnt path; do
        if [[ "$cnt" -eq 1 && "$path" != */ ]]; then
          echo "  + $path" >&2
        else
          echo "  + $path ($cnt files)" >&2
        fi
      done
  fi
else
  echo "No .worktreeinclude file found - skipping file copy" >&2
fi

echo "$(cd "$WORKTREE_PATH" && pwd)"
