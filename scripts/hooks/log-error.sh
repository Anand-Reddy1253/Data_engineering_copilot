#!/usr/bin/env bash
# log-error.sh — Error logging hook for agent error events
# Reads error JSON from stdin and appends to .agent-logs/errors.log

set -uo pipefail

INPUT=$(cat)

TIMESTAMP=$(date -u +%Y-%m-%dT%H:%M:%SZ)
ERROR_MSG=$(echo "$INPUT" | jq -r '.error // .message // "Unknown error"' 2>/dev/null || echo "Unknown error")
TOOL_NAME=$(echo "$INPUT" | jq -r '.toolName // "unknown"' 2>/dev/null || echo "unknown")

mkdir -p .agent-logs

LOG_ENTRY=$(echo "$INPUT" | jq --arg ts "$TIMESTAMP" '. + {"logged_at": $ts}' 2>/dev/null || echo "{\"timestamp\": \"$TIMESTAMP\", \"error\": \"$ERROR_MSG\", \"tool\": \"$TOOL_NAME\"}")

echo "$LOG_ENTRY" >> .agent-logs/errors.log

echo "{\"status\": \"logged\", \"timestamp\": \"$TIMESTAMP\", \"error\": $(echo "$ERROR_MSG" | jq -Rs .)}"
exit 0
