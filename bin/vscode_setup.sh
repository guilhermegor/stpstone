#!/bin/bash

sed -i 's/\r//g' .vscode/extensions.txt
grep '^vscode:' .vscode/extensions.txt | while IFS= read -r extension; do
    code --install-extension "${extension#vscode:}"
done
