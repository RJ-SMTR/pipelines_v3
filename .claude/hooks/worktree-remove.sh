#!/bin/bash
# WorktreeRemove hook — cleans up worktree when session ends
# Input (stdin JSON): { "worktree_path": "<absolute-path>" }

set -e

INPUT=$(cat)
WORKTREE_PATH=$(echo "$INPUT" | jq -r '.worktree_path')

[[ -z "$WORKTREE_PATH" || "$WORKTREE_PATH" == "null" ]] && exit 0
[[ ! -d "$WORKTREE_PATH" ]] && exit 0

if git worktree remove "$WORKTREE_PATH" --force 2>/dev/null; then
  echo "Removed worktree: $WORKTREE_PATH" >&2
else
  echo "git worktree remove failed, attempting manual cleanup..." >&2
  git worktree prune 2>/dev/null || true
  rm -rf "$WORKTREE_PATH" 2>/dev/null || true
fi
