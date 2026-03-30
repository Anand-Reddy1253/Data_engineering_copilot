#!/usr/bin/env bash
# block-secrets.sh — Pre-tool-use hook to block operations containing secrets
# Reads JSON from stdin, checks toolInput for credential patterns, outputs decision

set -uo pipefail

# Read stdin JSON
INPUT=$(cat)

# Extract tool input as string for pattern matching
TOOL_INPUT=$(echo "$INPUT" | jq -r '.toolInput // {} | tostring' 2>/dev/null || echo "")
TOOL_NAME=$(echo "$INPUT" | jq -r '.toolName // ""' 2>/dev/null || echo "")

# Secret patterns to detect
PATTERNS=(
    "password\s*[=:]\s*['\"][^'\"]{3,}"
    "secret\s*[=:]\s*['\"][^'\"]{3,}"
    "api_key\s*[=:]\s*['\"][^'\"]{8,}"
    "token\s*[=:]\s*['\"][^'\"]{10,}"
    "private_key\s*[=:]\s*-----BEGIN"
    "aws_secret_access_key\s*[=:]"
    "AKIA[0-9A-Z]{16}"
)

VIOLATION_FOUND=false
VIOLATION_REASON=""

for pattern in "${PATTERNS[@]}"; do
    if echo "$TOOL_INPUT" | grep -qiE "$pattern" 2>/dev/null; then
        VIOLATION_FOUND=true
        VIOLATION_REASON="Potential secret detected matching pattern: $pattern"
        break
    fi
done

if [ "$VIOLATION_FOUND" = "true" ]; then
    # Output deny decision
    echo "{\"decision\": \"deny\", \"reason\": \"Security block: $VIOLATION_REASON. Use environment variables instead of hardcoded credentials.\"}"
    exit 0
fi

# Allow the tool use
echo "{\"decision\": \"allow\"}"
exit 0
