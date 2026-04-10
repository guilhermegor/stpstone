#!/usr/bin/env bash

set -euo pipefail

# ============================================================================
# CONSTANTS
# ============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${PROJECT_ROOT}/.env"
ENV_EXAMPLE="${PROJECT_ROOT}/.env.example"
LOG_FILE="${PROJECT_ROOT}/claude_login_$(date +%Y%m%d_%H%M%S).log"

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

print_status() {
	local status="$1"
	local message="$2"
	
	case "$status" in
		"success")
			echo -e "${GREEN}[✓]${NC} ${message}"
			;;
		"error")
			echo -e "${RED}[✗]${NC} ${message}" >&2
			;;
		"warning")
			echo -e "${YELLOW}[!]${NC} ${message}"
			;;
		"info")
			echo -e "${BLUE}[i]${NC} ${message}"
			;;
		"config")
			echo -e "${CYAN}[→]${NC} ${message}"
			;;
		"section")
			echo -e "\n${MAGENTA}========================================${NC}"
			echo -e "${MAGENTA} $message${NC}"
			echo -e "${MAGENTA}========================================${NC}\n"
			;;
		*)
			echo -e "[ ] ${message}"
			;;
	esac
	
	# Log to file
	echo "[$(date '+%Y-%m-%d %H:%M:%S')] [$status] $message" >> "$LOG_FILE"
}

# ============================================================================
# VALIDATION FUNCTIONS
# ============================================================================

check_env_file() {
	print_status "info" "Checking for .env file at ${ENV_FILE}..."
	
	if [[ ! -f "${ENV_FILE}" ]]; then
		print_status "error" ".env file not found"
		print_status "info" "Creating .env from template..."
		
		if [[ -f "${ENV_EXAMPLE}" ]]; then
			cp "${ENV_EXAMPLE}" "${ENV_FILE}"
			print_status "success" ".env file created from .env.example"
			print_status "warning" "Please edit .env and add your Claude Code credentials"
			print_status "config" "Required field: CLAUDE_API_KEY"
			print_status "config" "Optional fields: CLAUDE_ORG_ID, CLAUDE_EMAIL"
			return 1
		else
			print_status "error" ".env.example not found. Cannot create .env file."
			print_status "info" "Please create .env.example first"
			return 1
		fi
	fi
	
	print_status "success" ".env file found"
	return 0
}

load_credentials() {
	print_status "info" "Loading credentials from .env file..."
	
	# Source the .env file
	set -a
	# shellcheck source=/dev/null
	source "${ENV_FILE}"
	set +a
	
	# Validate required credentials
	local missing_creds=0
	
	if [[ -z "${CLAUDE_API_KEY:-}" ]]; then
		print_status "error" "CLAUDE_API_KEY is not set in .env file"
		missing_creds=1
	else
		# Mask the API key for display
		local masked_key="${CLAUDE_API_KEY:0:8}...${CLAUDE_API_KEY: -4}"
		print_status "config" "CLAUDE_API_KEY found: ${masked_key}"
	fi
	
	if [[ -n "${CLAUDE_ORG_ID:-}" ]]; then
		print_status "config" "CLAUDE_ORG_ID found: ${CLAUDE_ORG_ID}"
	fi
	
	if [[ -n "${CLAUDE_EMAIL:-}" ]]; then
		print_status "config" "CLAUDE_EMAIL found: ${CLAUDE_EMAIL}"
	else
		print_status "warning" "CLAUDE_EMAIL is not set (may be optional)"
	fi
	
	if [[ ${missing_creds} -eq 1 ]]; then
		print_status "error" "Missing required credentials"
		print_status "info" "Please update your .env file with CLAUDE_API_KEY"
		print_status "info" "See .env.example for reference"
		return 1
	fi
	
	print_status "success" "Credentials loaded successfully"
	return 0
}

# ============================================================================
# AUTHENTICATION FUNCTIONS
# ============================================================================

authenticate_with_credentials() {
	print_status "section" "AUTHENTICATING WITH STORED CREDENTIALS"
	
	# Export environment variables for the CLI
	export ANTHROPIC_API_KEY="${CLAUDE_API_KEY}"
	print_status "success" "Environment variable set: ANTHROPIC_API_KEY"
	
	if [[ -n "${CLAUDE_ORG_ID:-}" ]]; then
		export ANTHROPIC_ORG_ID="${CLAUDE_ORG_ID}"
		print_status "success" "Environment variable set: ANTHROPIC_ORG_ID"
	fi
	
	# Check if claude CLI is available
	if ! command -v claude &> /dev/null; then
		print_status "warning" "Claude CLI not found in PATH"
		print_status "info" "API key is available in environment variables"
		print_status "info" "You can use the Anthropic API directly"
		print_status "success" "Credentials ready for use"
		return 0
	fi
	
	print_status "info" "Claude CLI detected, attempting authentication..."
	
	# Attempt to authenticate using the CLI
	if claude auth login --api-key "${CLAUDE_API_KEY}" 2>/dev/null; then
		print_status "success" "Successfully authenticated with Claude Code CLI"
		return 0
	else
		print_status "warning" "CLI authentication failed"
		print_status "info" "API key is still available in environment variables"
		print_status "success" "You can use the API key directly in your code"
		return 0
	fi
}

standard_login() {
	print_status "section" "STANDARD LOGIN PROCESS"
	
	if ! command -v claude &> /dev/null; then
		print_status "error" "Claude CLI not found in PATH"
		print_status "info" "Alternative: Set CLAUDE_API_KEY in .env file"
		print_status "info" "Get your API key from: https://console.anthropic.com/settings/keys"
		return 1
	fi
	
	print_status "info" "Opening interactive login..."
	
	if claude auth login; then
		print_status "success" "Successfully logged in to Claude Code"
		return 0
	else
		print_status "error" "Login failed"
		print_status "info" "Please try again or use API key in .env file"
		return 1
	fi
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

main() {
	print_status "section" "CLAUDE CODE AUTHENTICATION"
	print_status "info" "Log file: ${LOG_FILE}"
	
	# Step 1: Check if .env file exists
	if ! check_env_file; then
		print_status "error" "Cannot proceed without .env file"
		exit 1
	fi
	
	# Step 2: Try to load credentials from .env
	if load_credentials; then
		# Step 3: Authenticate using credentials
		if authenticate_with_credentials; then
			print_status "section" "AUTHENTICATION COMPLETE"
			print_status "success" "Ready to use Claude Code API"
			exit 0
		else
			print_status "warning" "Credential-based authentication incomplete"
			print_status "info" "Attempting standard login process..."
			
			if standard_login; then
				exit 0
			else
				exit 1
			fi
		fi
	else
		# Step 4: Fall back to standard login
		print_status "warning" "Credential validation failed"
		print_status "info" "Attempting standard login process..."
		
		if standard_login; then
			exit 0
		else
			exit 1
		fi
	fi
}

# Run main function
main
