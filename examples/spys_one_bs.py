import requests
import re
from bs4 import BeautifulSoup
from pathlib import Path

def ensure_directory_exists():
    """Ensure the data directory exists"""
    Path("data").mkdir(exist_ok=True)

def save_html(content):
    """Save HTML content to file"""
    ensure_directory_exists()
    with open("data/spys_proxies.html", "w", encoding="utf-8") as f:
        f.write(str(content))  # Convert Tag to string before writing

def get_proxies(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')

        proxies = []
        # Find all script tags that contain port decoding information
        scripts = soup.find_all('script')
        port_decode = {}

        # First pass: Extract port decoding variables
        for script in scripts:
            if '^' in script.text and '=' in script.text:
                for line in script.text.split(';'):
                    if '^' in line and '=' in line:
                        try:
                            var, expr = line.split('=', 1)
                            var = var.strip()
                            port_decode[var] = expr.strip()
                        except:
                            continue

        # The proxy list is in a table with class 'spy1x'
        tables = soup.find_all('table')
        for table in tables:
            if table:
                save_html(table)
                rows = table.find_all('tr')
                for row in rows:
                    cols = row.find_all('td')
                    # The IP is in the first column, port is in the second
                    ip_script = cols[0].find('font', class_='spy14')
                    if ip_script:
                        # The IP is hidden in JavaScript, we'll extract it
                        ip_text = ip_script.text
                        # This is a simple way to extract the IP - might need adjustment
                        ip_parts = []
                        for part in ip_text.split('+'):
                            part = part.strip().strip("'")
                            if part and part != 'document.write':
                                ip_parts.append(part)
                        ip = ''.join(ip_parts).split(':')[0]
                        # Extract port script
                        port_script = cols[0].find('script')
                        if port_script:
                            # Get the port expression (e.g., (Two3NineEight^ZeroFiveNine)+...)
                            port_expr = re.search(r'document\.write\(":"\+([^)]+)\)', port_script.text)
                            if port_expr:
                                port_parts = port_expr.group(1).split('+')
                                port = ''
                                for part in port_parts:
                                    part = part.strip('()')
                                    if part in port_decode:
                                        # Evaluate the port part (e.g., "Two3NineEight^ZeroFiveNine")
                                        try:
                                            port += str(eval(port_decode[part]))
                                        except:
                                            pass

                                if ip and port:
                                    proxies.append(f"{ip}:{port}")
                        raise Exception("BREAKPOINT")

        return proxies

    except Exception as e:
        print(f"Error fetching proxies: {e}")
        return []

# URL for Brazil free proxy list
url = "https://spys.one/free-proxy-list/BR/"
proxies = get_proxies(url)

print(f"Found {len(proxies)} proxies:")
for proxy in proxies[:10]:  # Print first 10 proxies
    print(proxy)
