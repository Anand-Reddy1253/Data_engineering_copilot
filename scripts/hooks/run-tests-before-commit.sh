#!/usr/bin/env bash
# run-tests-before-commit.sh — Run unit tests before allowing a commit
set -uo pipefail

echo "Running tests before commit..."

# Check if virtual environment is active or available
if [ -d ".venv" ]; then
    source .venv/bin/activate 2>/dev/null || true
fi

# Run unit tests (fast only, skip integration)
if command -v pytest >/dev/null 2>&1; then
    pytest tests/unit/ -q --tb=short -x 2>&1
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        echo "Tests failed. Commit blocked. Fix tests before committing."
        exit 1
    fi
    echo "All unit tests passed. Commit allowed."
else
    echo "pytest not found. Skipping test check."
fi

exit 0
