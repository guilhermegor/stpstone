#!/bin/bash

sed -i 's/\r//g' requirements-dev.txt
grep '^vscode:' requirements-dev.txt | while IFS= read -r extension; do
    code --install-extension "${extension#vscode:}"
done
