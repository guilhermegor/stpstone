# Claude Code Authentication Guide

## Overview

This project uses `.env` file for Claude Code API authentication. **Important**: The Claude CLI and API usage have different authentication mechanisms.

## Quick Start

```bash
# 1. Create .env from template
make claude_login

# 2. Edit .env and add your API key
vim .env  # Add CLAUDE_API_KEY=sk-ant-...

# 3. Run authentication setup
make claude_login
```

## Understanding Authentication

### Two Separate Authentication Systems

1. **API Key (Environment Variable)**
   - Variable: `ANTHROPIC_API_KEY`
   - Used for: Python SDK, direct API calls, automation scripts
   - Set by: `.env` file → exported by `make claude_login`
   - Works with: `anthropic` Python package, API requests

2. **Claude CLI Session**
   - Used for: Interactive `claude` command
   - Set by: `claude auth login` or `/login` inside CLI
   - Uses: Account credentials (email/password or SSO)
   - **Does NOT use API key from .env**

## Usage Examples

### Python SDK (Uses API Key)

```python
# This uses ANTHROPIC_API_KEY from environment
from anthropic import Anthropic

client = Anthropic()  # Automatically picks up API key
response = client.messages.create(
    model="claude-sonnet-4",
    messages=[{"role": "user", "content": "Hello!"}]
)
```

### Direct API Calls (Uses API Key)

```bash
# Environment variable from .env is used
curl https://api.anthropic.com/v1/messages \
  -H "x-api-key: $ANTHROPIC_API_KEY" \
  -H "anthropic-version: 2023-06-01" \
  -d '{"model":"claude-sonnet-4","messages":[{"role":"user","content":"Hello"}]}'
```

### Claude CLI (Separate Authentication)

```bash
# Start CLI
claude

# Inside CLI, authenticate with account
/login

# Or from terminal
claude auth login
```

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `CLAUDE_API_KEY` | Yes | API key from console.anthropic.com |
| `CLAUDE_ORG_ID` | No | Organization ID (if applicable) |
| `CLAUDE_EMAIL` | No | Reference only (not used for auth) |
| `CLAUDE_MODEL` | No | Default model to use |
| `CLAUDE_MAX_TOKENS` | No | Default max tokens |

## Troubleshooting

### "Not logged in" in Claude CLI

**Problem**: CLI shows "Not logged in" even after `make claude_login`

**Solution**: The CLI needs separate authentication:
```bash
claude auth login
# OR
claude
/login
```

### API key works but CLI doesn't

This is expected! They use different auth systems:
- ✓ API key works for: Python scripts, API calls
- ✗ API key does NOT work for: `claude` CLI

### Switching accounts

For API usage:
```bash
# Edit .env with new API key
vim .env
make claude_login
```

For CLI:
```bash
claude auth logout
claude auth login
```

## Security

- ✗ **NEVER** commit `.env` file (already in `.gitignore`)
- ✓ Keep API keys confidential
- ✓ Use different keys for different environments
- ✓ Rotate keys periodically

## See Also

- [Anthropic API Documentation](https://docs.anthropic.com/)
- [API Keys Console](https://console.anthropic.com/settings/keys)
- Claude CLI: Run `claude --help`
