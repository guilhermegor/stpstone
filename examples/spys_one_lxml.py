import requests
from lxml import html
from pathlib import Path

url = "https://spys.one/free-proxy-list/BR/"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': 'https://www.google.com/',
}

def ensure_directory_exists():
    """Ensure the data directory exists"""
    Path("data").mkdir(exist_ok=True)

def save_html(content):
    """Save HTML content to file"""
    ensure_directory_exists()
    with open("data/spys_proxies.html", "wb") as f:
        f.write(content)

def scrape_proxies():
    try:
        resp_req = requests.get(url, headers=headers, timeout=15)
        resp_req.raise_for_status()
        page = resp_req.content
        save_html(page)
        html_content = html.fromstring(page)

        # Find all proxy rows - they have class spy1x or spy1xx
        proxy_rows = html_content.xpath('//tr[contains(@class, "spy1x") or contains(@class, "spy1xx")]')

        list_proxies = []
        for row in proxy_rows:
            # Extract IP address (before the script tag)
            ip_element = row.xpath('.//font[@class="spy14"]/text()[1]')
            ip = ip_element[0].strip() if ip_element else None

            # Extract port (from the script)
            port_script = row.xpath('.//font[@class="spy14"]/text()[1]')
            port = port_script[0].strip() if port_script else None

            # Extract proxy type (HTTP/HTTPS/SOCKS)
            proxy_type_element = row.xpath('.//a[contains(@href, "proxy-list")]/font[@class="spy1"]/text()')
            proxy_type = proxy_type_element[0] if proxy_type_element else None

            # Extract anonymity level
            anonymity_element = row.xpath('.//font[@class="spy5" or @class="spy1" or @class="spy2"]/text()')
            anonymity = anonymity_element[0] if anonymity_element else None

            # Extract country/region
            region_element = row.xpath('.//font[@class="spy14"]/following-sibling::font[@class="spy1"]/text()')
            region = region_element[0] if region_element else None

            # Extract latency
            latency_element = row.xpath('.//font[@class="spy1"]/text()[contains(., ".")]')
            latency = latency_element[0] if latency_element else None

            # Extract uptime
            uptime_element = row.xpath('.//acronym[contains(@title, "last check status")]/text()')
            uptime = uptime_element[0] if uptime_element else None

            # Extract check date
            date_element = row.xpath('.//font[@class="spy14"]/text()[last()]')
            check_date = date_element[0] if date_element else None

            if ip:
                list_proxies.append({
                    "ip": ip,
                    "port": port,
                    "proxy_type": proxy_type,
                    "anonymity": anonymity,
                    "region": region,
                    "latency": latency,
                    "uptime": uptime,
                    "check_date": check_date
                })

        return list_proxies

    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return []
    except Exception as e:
        print(f"Error: {e}")
        return []

if __name__ == "__main__":
    proxies = scrape_proxies()
    print(proxies)
