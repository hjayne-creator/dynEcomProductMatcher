import re
import time
from typing import Dict, Optional

import requests


DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def get_domain(url: str) -> str:
    return re.sub(r"^https?://", "", url).split("/")[0].lower()


def http_get(url: str, timeout: int = 30, max_retries: int = 2) -> Optional[str]:
    last_exc: Optional[Exception] = None
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
            if resp.status_code == 200 and "text/html" in resp.headers.get("Content-Type", ""):
                return resp.text
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
        time.sleep(0.5 * (attempt + 1))
    if last_exc:
        return None
    return None


def clean_title(title: str) -> str:
    if not title:
        return ""
    # Remove common separators and store suffixes
    title = re.sub(r"\s+[\-|–|\|]\s+.*$", "", title).strip()
    # Collapse whitespace
    title = re.sub(r"\s+", " ", title)
    return title[:200]


IDENTIFIER_REGEXES = {
    "gtin": re.compile(r"\b(?:gtin|ean|upc)[\s#:]*([0-9]{8,14})\b", re.I),
    "mpn": re.compile(r"\bmpn[\s#:]*([\w\-\.]{3,})\b", re.I),
    "sku": re.compile(r"\bsku[\s#:]*([\w\-\.]{3,})\b", re.I),
}


def extract_identifiers(text: str) -> Dict[str, str]:
    ids: Dict[str, str] = {}
    if not text:
        return ids
    for key, pattern in IDENTIFIER_REGEXES.items():
        m = pattern.search(text)
        if m:
            ids[key] = m.group(1)
    return ids


