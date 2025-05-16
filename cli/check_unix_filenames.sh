#!/bin/bash

for f in "$@"; do
  # Ignore .git/* files
  if [[ "$f" == .git/* ]]; then
    continue
  fi

  if [[ "$f" == *"/"* ]]; then
    echo "Error: Filename contains slash: $f"
    exit 1
  fi
done
