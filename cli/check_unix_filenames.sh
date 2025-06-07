#!/bin/bash
# cli/check_unix_filenames.sh

for f in "$@"; do
  # skip directories and .git files
  if [[ -d "$f" ]] || [[ "$f" == .git/* ]]; then
    continue
  fi

  # get just the filename without path
  filename=$(basename "$f")

  # check for invalid characters (excluding slashes in paths)
  if [[ "$filename" == *[^a-zA-Z0-9._-]* ]]; then
    echo "Error: Invalid filename '$filename' in path: $f"
    echo "Only alphanumeric, ., - and _ are allowed in filenames"
    exit 1
  fi
done
