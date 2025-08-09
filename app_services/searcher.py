import os
import re
from typing import Dict, List, Set

import requests

from .utils import get_domain


SERP_API_URL = "https://serpapi.com/search.json"


def build_queries(signals: Dict) -> List[str]:
    title = signals.get("title") or ""
    brand = signals.get("brand") or ""
    ids = signals.get("identifiers") or {}
    model = ids.get("model") or ids.get("mpn") or ""

    base = " ".join([x for x in [brand, model] if x]).strip()
    q1 = f'"{title}"' if title else base
    q2 = f"{brand} {model}".strip() if base else title
    q3 = f"{title} {brand}".strip()

    queries = list({q for q in [q1, q2, q3] if q})
    return queries[:3]


def _serpapi_search(query: str, num: int = 10) -> List[str]:
    api_key = os.environ.get("SERPAPI_KEY")
    if not api_key:
        return []
    params = {
        "engine": "google",
        "q": query,
        "num": num,
        "api_key": api_key,
        "hl": "en",
        "gl": "us",
    }
    try:
        resp = requests.get(SERP_API_URL, params=params, timeout=25)
        data = resp.json()
        results = []
        for item in data.get("organic_results", []):
            link = item.get("link")
            if link:
                results.append(link)
        return results
    except Exception:  # noqa: BLE001
        return []


NON_PRODUCT_PATTERNS = [
    re.compile(p, re.I)
    for p in [
        r"/search",
        r"/collections",
        r"/category",
        r"/c/",
        r"/s\?",
        r"/cart",
        r"/account",
        r"/help",
        r"/blog",
    ]
]


def _looks_like_product_url(url: str) -> bool:
    return not any(p.search(url) for p in NON_PRODUCT_PATTERNS)


def search_candidates(queries: List[str], original_url: str) -> List[str]:
    original_domain = get_domain(original_url)
    candidates: List[str] = []
    seen_domains: Set[str] = set()

    for q in queries:
        # Exclude original domain
        qx = f"{q} -site:{original_domain}"
        for url in _serpapi_search(qx, num=10):
            if not _looks_like_product_url(url):
                continue
            d = get_domain(url)
            if d == original_domain:
                continue
            if d in seen_domains:
                continue
            seen_domains.add(d)
            candidates.append(url)

    return candidates[:20]


