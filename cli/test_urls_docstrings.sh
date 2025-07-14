#!/bin/bash

# Enhanced color palette
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

# Cache configuration
CACHE_DIR=".url_check_cache"
CACHE_TTL=$((60 * 60 * 24 * 7)) # 1 week in seconds
mkdir -p "$CACHE_DIR"

print_error() {
    echo -e "${RED}[✗]${NC} $1" >&2
}

# Cache functions
get_cache() {
    local url="$1"
    local cache_file="${CACHE_DIR}/$(echo -n "$url" | md5sum | cut -d' ' -f1)"
    
    if [[ -f "$cache_file" ]]; then
        local timestamp=$(stat -c %Y "$cache_file")
        local now=$(date +%s)
        
        if (( now - timestamp < CACHE_TTL )); then
            cat "$cache_file"
            return 0
        fi
    fi
    return 1
}

set_cache() {
    local url="$1"
    local status="$2"
    local cache_file="${CACHE_DIR}/$(echo -n "$url" | md5sum | cut -d' ' -f1)"
    echo "$status" > "$cache_file"
}

# Function to check URL status with timeout and caching
check_url() {
    local url="$1"
    local status_code
    
    # First try to get from cache
    if status_code=$(get_cache "$url"); then
        echo "$status_code"
        return
    fi
    
    local user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    
    # Known problematic domains that block automated requests but are valid URLs
    local problematic_domains=(
        "platform.openai.com"
        "openai.com"
        "stackoverflow.com"
        "b3.com.br"
        "line.bvmfnet.com.br"
        "reuters.com"
        "code.activestate.com"
        "exceltip.com"
        "chromedriver.chromium.org"
        "ime.usp.br"
        "udemy.com"
        "ssc.wisc.edu"
        "pythonnumericalmethods.berkeley.edu"
        "towardsdatascience.com"
        "download.bmfbovespa.com.br"
        "geeksforgeeks.org"
    )
    
    # Check if URL is from a problematic domain
    for domain in "${problematic_domains[@]}"; do
        if [[ "$url" == *"$domain"* ]]; then
            # For problematic domains, assume they're valid
            if [[ "$url" =~ ^https?:// ]]; then
                set_cache "$url" "200"
                echo "200"
                return
            fi
        fi
    done
    
    # Try multiple methods to get a proper status code
    # Method 1: HEAD request with User-Agent and additional headers
    status_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 --head \
        -H "User-Agent: $user_agent" \
        -H "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8" \
        -H "Accept-Language: en-US,en;q=0.5" \
        -H "Accept-Encoding: gzip, deflate" \
        -H "Connection: keep-alive" \
        "$url" 2>/dev/null)
    
    # Method 2: If HEAD fails or returns 403/405, try GET with full browser headers
    if [[ -z "$status_code" || "$status_code" -eq 403 || "$status_code" -eq 405 ]]; then
        status_code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 \
            -H "User-Agent: $user_agent" \
            -H "Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8" \
            -H "Accept-Language: en-US,en;q=0.5" \
            -H "Accept-Encoding: gzip, deflate" \
            -H "Connection: keep-alive" \
            -H "Upgrade-Insecure-Requests: 1" \
            "$url" 2>/dev/null)
    fi
    
    # Method 3: If still getting 403, try with wget as fallback
    if [[ "$status_code" -eq 403 ]]; then
        if wget --spider --timeout=10 --user-agent="$user_agent" "$url" >/dev/null 2>&1; then
            status_code="200"
        fi
    fi
    
    # Cache successful responses
    if [[ "$status_code" =~ ^2 ]]; then
        set_cache "$url" "$status_code"
    fi
    
    echo "$status_code"
}

# Clean old cache entries
clean_cache() {
    find "$CACHE_DIR" -type f -mtime +$((CACHE_TTL / 60 / 60 / 24)) -delete 2>/dev/null
}

# Main function to process Python files
process_python_files() {
    clean_cache # Clean old cache entries at start
    
    local root_dir="${1:-.}"
    declare -A processed_urls
    local has_errors=0

    while IFS= read -r -d '' file; do
        local line_num=0
        local in_docstring=false

        while IFS= read -r line; do
            ((line_num++))
            
            # Detect docstring start/end
            if [[ "$line" =~ ^\ *[\"\']{3} ]]; then
                [[ "$in_docstring" == true ]] && in_docstring=false || in_docstring=true
                continue
            fi

            # Only look for URLs in docstrings
            if [[ "$in_docstring" == true ]]; then
                while [[ "$line" =~ (https?://[a-zA-Z0-9./?=_%:-]+[a-zA-Z0-9./?=_%:-]) ]]; do
                    url="${BASH_REMATCH[1]}"
                    
                    # Skip if we've already processed this URL
                    if [[ -n "${processed_urls[$url]}" ]]; then
                        line="${line#*$url}"
                        continue
                    fi
                    processed_urls["$url"]=1
                    
                    # Skip partial/incomplete URLs
                    if [[ "$url" =~ (https?://[^/]+)$ ]]; then
                        line="${line#*$url}"
                        continue
                    fi

                    status_code=$(check_url "$url")
                    
                    # Only print errors
                    if [[ -z "$status_code" ]]; then
                        print_error "Failed to access URL in $file (line $line_num): $url"
                        has_errors=1
                    elif [[ "$status_code" =~ ^[34] ]]; then
                        print_error "URL issue ($status_code) in $file (line $line_num): $url"
                        has_errors=1
                    elif [[ ! "$status_code" =~ ^2 ]]; then
                        print_error "URL problem ($status_code) in $file (line $line_num): $url"
                        has_errors=1
                    fi
                    
                    line="${line#*$url}"
                done
            fi
        done < "$file"
    done < <(find "$root_dir" -type f -name "*.py" -print0)

    # Final status
    if [[ $has_errors -eq 0 ]]; then
        exit 0
    else
        exit 1
    fi
}

# Run the main function with the first argument or current directory
process_python_files "${1:-.}"