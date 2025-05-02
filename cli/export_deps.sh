#!/bin/bash

set -e

if [[ "$OS" == "Windows_NT" ]]; then
    cmd /c "poetry export -f requirements.txt --output requirements.txt --without-hashes && (for /f %i in ('code --list-extensions') do @echo vscode:%i) > requirements-dev.txt && git add requirements.txt requirements-dev.txt"
else
    poetry export -f requirements.txt --output requirements-venv.txt --without-hashes
    code --list-extensions | sed 's/^/vscode:/' > requirements-dev.txt
    git add requirements-venv.txt requirements-dev.txt
fi
