#!/bin/bash
set -eo pipefail

# configuration
REQUIREMENTS_FILE="requirements-venv.txt"
REQUIREMENTS_DEV_FILE="requirements-dev.txt"
VSCODE_PREFIX="vscode:"

# function to handle errors
handle_error() {
    echo "Error: $1" >&2
    exit 1
}

# check if poetry is available
if ! command -v poetry &> /dev/null; then
    handle_error "poetry not found. Please install Poetry first."
fi

# export main dependencies
echo "Exporting production dependencies to $REQUIREMENTS_FILE..."
if ! poetry export -f requirements.txt --output "$REQUIREMENTS_FILE" --without-hashes; then
    echo "Warning: Failed to export with poetry, falling back to pip freeze" >&2
    pip freeze > "$REQUIREMENTS_FILE"
fi

# export development dependencies (vscode extensions)
if command -v code &> /dev/null; then
    echo "Exporting VSCode extensions to $REQUIREMENTS_DEV_FILE..."
    code --list-extensions | awk -v prefix="$VSCODE_PREFIX" '{print prefix $0}' > "$REQUIREMENTS_DEV_FILE"
else
    echo "VSCode not found, skipping extensions export" >&2
    touch "$REQUIREMENTS_DEV_FILE"
fi

# add to git if in a repository
if git rev-parse --is-inside-work-tree &> /dev/null; then
    git add "$REQUIREMENTS_FILE" "$REQUIREMENTS_DEV_FILE" || \
    echo "Warning: Failed to add files to git" >&2
else
    echo "Not in a git repository, skipping git add" >&2
fi

echo "Dependency export completed successfully"
