#!/usr/bin/env bash
input=$(cat)

# Extração de dados com jq
MODEL=$(echo "$input" | jq -r '.model.display_name')
DIR=$(echo "$input" | jq -r '.workspace.current_dir')
COST=$(echo "$input" | jq -r '.cost.total_cost_usd // 0')
PCT=$(echo "$input" | jq -r '.context_window.used_percentage // 0' | cut -d. -f1)
DURATION_MS=$(echo "$input" | jq -r '.cost.total_duration_ms // 0')

# Novas métricas de Quota e Pensamento
QUOTA_PCT=$(echo "$input" | jq -r '.rate_limits.five_hour.used_percentage // 0' | cut -d. -f1)
QUOTA_RESET=$(echo "$input" | jq -r '.rate_limits.five_hour.resets_at // 0')
THINKING=$(echo "$input" | jq -r '.thinking.enabled // false')
EFFORT=$(echo "$input" | jq -r '.effort.level // "none"')
EXCEEDS_200K=$(echo "$input" | jq -r '.exceeds_200k_tokens // false')
SESSION=$(echo "$input" | jq -r '.session_name // ""')

# Cores
CYAN='\033[36m'; GREEN='\033[32m'; YELLOW='\033[33m'; RED='\033[31m'; PURPLE='\033[35m'; RESET='\033[0m'

# Lógica de cores para Contexto e Quota
[ "$PCT" -ge 80 ] && CTX_COLOR="$RED" || CTX_COLOR="$GREEN"
[ "$QUOTA_PCT" -ge 80 ] && QUOTA_COLOR="$RED" || QUOTA_COLOR="$CYAN"

# Formatação do Tempo de Reset
RESET_TIME=""
if [ "$QUOTA_RESET" -gt 0 ]; then
    RESET_TIME=" (reset $(date -d @$QUOTA_RESET +%H:%M))"
fi

# Indicador de Thinking
THINK_STATUS=""
if [ "$THINKING" = "true" ]; then
    THINK_STATUS=" 🧠 ${PURPLE}${EFFORT}${RESET} |"
fi

# Alerta de Contexto Grande
WARN_200K=""
[ "$EXCEEDS_200K" = "true" ] && WARN_200K="${RED} [!200K]${RESET}"

# Nome da Sessão
SESSION_STR=""
[ -n "$SESSION" ] && SESSION_STR=" 🔖 ${SESSION} |"

# Cálculos de Tempo e Custo
MINS=$((DURATION_MS / 60000)); SECS=$(((DURATION_MS % 60000) / 1000))
COST_FMT=$(printf '$%.2f' "$COST")

# Git info (preservando contagem de staged/modified)
GIT_INFO=""
if git -C "$DIR" rev-parse --git-dir > /dev/null 2>&1; then
    BRANCH=$(git -C "$DIR" --no-optional-locks branch --show-current 2>/dev/null)
    STAGED=$(git -C "$DIR" --no-optional-locks diff --cached --numstat 2>/dev/null | wc -l | tr -d ' ')
    MODIFIED=$(git -C "$DIR" --no-optional-locks diff --numstat 2>/dev/null | wc -l | tr -d ' ')

    GIT_STATUS=""
    [ "$STAGED" -gt 0 ] && GIT_STATUS="${GREEN}+${STAGED}${RESET}"
    [ "$MODIFIED" -gt 0 ] && GIT_STATUS="${GIT_STATUS}${YELLOW}~${MODIFIED}${RESET}"
    [ -n "$GIT_STATUS" ] && GIT_STATUS=" ${GIT_STATUS}"

    GIT_INFO=" | 🌿 ${BRANCH}${GIT_STATUS}"
fi

# Barra de Contexto ASCII
FILLED=$((PCT / 10)); EMPTY=$((10 - FILLED))
BAR=""; for ((i=0; i<FILLED; i++)); do BAR+="█"; done; for ((i=0; i<EMPTY; i++)); do BAR+="░"; done

# Saída Final
echo -e "${CYAN}[${MODEL}]${RESET} |${SESSION_STR}${THINK_STATUS} 📁 ${DIR##*/}${GIT_INFO}${WARN_200K}"
echo -e "${CTX_COLOR}${BAR}${RESET} ${PCT}% | 🎫 Quota: ${QUOTA_COLOR}${QUOTA_PCT}%${RESET}${RESET_TIME} | 💰 ${YELLOW}${COST_FMT}${RESET} | ⏱️ ${MINS}m${SECS}s"
