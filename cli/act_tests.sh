#!/bin/bash

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Config
WORKFLOW_FILE=".github/workflows/tests.yaml"
ACT_IMAGE="ghcr.io/catthehacker/ubuntu:act-24.04"
PYTHON_VERSION="3.12.8"

# Fix permissions in act container
fix_act_permissions() {
    echo -e "${YELLOW}⚙ Fixing permissions in act container...${NC}"
    docker exec $(docker ps -q -f name=act-) chmod -R 755 /var/run/act || true
    docker exec $(docker ps -q -f name=act-) chmod +x /var/run/act/actions/*/*.sh || true
}

# Main test function
run_workflow_test() {
    echo -e "${BLUE}=== Running Workflow Test ===${NC}"

    # First attempt (may fail due to permissions)
    echo -e "${YELLOW}Attempt 1/2: Running workflow...${NC}"
    act -W "$WORKFLOW_FILE" \
        -P ubuntu-24.04="$ACT_IMAGE" \
        -j test \
        -s PYTHON_VERSION="$PYTHON_VERSION" \
        --container-options "--privileged" \
        -v

    if [ $? -ne 0 ]; then
        echo -e "${YELLOW}⚠ First attempt failed, fixing permissions and retrying...${NC}"
        fix_act_permissions

        echo -e "${YELLOW}Attempt 2/2: Retrying workflow...${NC}"
        act -W "$WORKFLOW_FILE" \
            -P ubuntu-24.04="$ACT_IMAGE" \
            -j test \
            -s PYTHON_VERSION="$PYTHON_VERSION" \
            --container-options "--privileged" \
            -v

        if [ $? -ne 0 ]; then
            echo -e "${RED}✖ Workflow failed after retry${NC}"
            return 1
        fi
    fi

    return 0
}

# Main execution
echo -e "${BLUE}=== GitHub Actions Local Tester ===${NC}"

# Verify requirements
if ! command -v act &> /dev/null; then
    echo -e "${RED}✖ act is not installed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ act $(act --version | awk '{print $3}') found${NC}"

if ! docker info &> /dev/null; then
    echo -e "${RED}✖ Docker is not running${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Docker is running${NC}"

if [ ! -f "$WORKFLOW_FILE" ]; then
    echo -e "${RED}✖ Workflow file not found: $WORKFLOW_FILE${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Found workflow: $WORKFLOW_FILE${NC}"

# Pull act image if needed
if ! docker image inspect "$ACT_IMAGE" &> /dev/null; then
    echo -e "${YELLOW}⚠ Pulling act runner image...${NC}"
    docker pull "$ACT_IMAGE"
fi
echo -e "${GREEN}✓ Using runner image: $ACT_IMAGE${NC}"

# Run the test
run_workflow_test

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✔ All tests completed successfully${NC}"
else
    echo -e "${RED}✖ Some tests failed${NC}"
    echo -e "${YELLOW}Debug tips:"
    echo -e "1. Try running with more verbose output: act -W $WORKFLOW_FILE -v -v"
    echo -e "2. Check container logs: docker logs <container-id>"
    echo -e "3. Enter container manually: docker exec -it <container-id> bash${NC}"
    exit 1
fi
