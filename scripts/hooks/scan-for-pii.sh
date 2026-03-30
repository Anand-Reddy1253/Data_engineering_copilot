#!/usr/bin/env bash
# scan-for-pii.sh — Post-tool-use hook to scan edited files for raw PII
# Reads JSON from stdin, checks files created/edited for PII patterns

set -uo pipefail

INPUT=$(cat)

# Get the file path from tool output if available
FILE_PATH=$(echo "$INPUT" | jq -r '.toolOutput.path // .toolInput.path // ""' 2>/dev/null || echo "")

# If no file path, check all recently modified Python files
if [ -z "$FILE_PATH" ]; then
    RECENT_FILES=$(find . -name "*.py" -newer .git/index 2>/dev/null | grep -v "__pycache__" | head -10 || echo "")
else
    RECENT_FILES="$FILE_PATH"
fi

PII_PATTERNS=(
    'logger\.(info|debug|warning|error).*\bemail\b'
    'logger\.(info|debug|warning|error).*\bphone\b'
    'logger\.(info|debug|warning|error).*\baddress\b'
    'print\(.*\bemail\b'
    'print\(.*\bphone\b'
)

VIOLATIONS=()

for file in $RECENT_FILES; do
    if [ -f "$file" ]; then
        for pattern in "${PII_PATTERNS[@]}"; do
            if grep -qiE "$pattern" "$file" 2>/dev/null; then
                VIOLATIONS+=("$file: possible PII in log statement (pattern: $pattern)")
            fi
        done
    fi
done

if [ ${#VIOLATIONS[@]} -gt 0 ]; then
    VIOLATION_MSG=$(printf '%s\n' "${VIOLATIONS[@]}" | jq -Rs .)
    echo "{\"level\": \"warning\", \"message\": \"Possible PII detected in log statements\", \"violations\": $VIOLATION_MSG}"
else
    echo "{\"level\": \"info\", \"message\": \"No PII detected in edited files\"}"
fi

exit 0
