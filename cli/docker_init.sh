#!/bin/bash

# detect the operating system
OS=$(uname -s)

# check if docker is running
if [[ "$OS" == "Linux" ]]; then
    # * linux: check if docker is running using pgrep (you may need to adjust permissions for sudo)
    if ! pgrep -x "dockerd" > /dev/null; then
        echo "Docker is not running. Starting Docker on Linux..."
        sudo systemctl start docker
    else
        echo "Docker is already running."
    fi
elif [[ "$OS" == "Darwin" ]]; then
    # * macos: check if docker is running using the 'docker' command
    if ! docker info > /dev/null 2>&1; then
        echo "Docker is not running. Starting Docker on macOS..."
        open -a Docker &
    else
        echo "Docker is already running."
    fi
elif [[ "$OS" == "MINGW32_NT"* || "$OS" == "MINGW64_NT"* || "$OS" == "CYGWIN_NT"* ]]; then
    # * windows: use 'docker info' to check if docker is running
    if ! docker info > /dev/null 2>&1; then
        echo "Docker is not running. Starting Docker Desktop on Windows..."
        powershell.exe -Command "Start-Process -FilePath 'C:\Program Files\Docker\Docker\Docker Desktop.exe' -WindowStyle Minimized"
    else
        echo "Docker is already running."
    fi
else
    echo "Unsupported OS: $OS"
fi

# wait for docker to be fully initialized
echo "Waiting 60 seconds for Docker to fully initialize..."
sleep 60

# docker CLI basic commands
docker --version
docker run --name hello-world hello-world
docker rm hello-world
docker rmi hello-world
