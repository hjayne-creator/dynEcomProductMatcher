import json
import re
from typing import Any, Dict, Optional

from bs4 import BeautifulSoup

from .utils import http_get, clean_title, extract_identifiers


def _parse_json_ld(soup: BeautifulSoup) -> Dict[str, Any]:
    data: Dict[str, Any] = {}
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            content = script.string or script.text
            if not content:
                continue
            obj = json.loads(content)
            items = obj if isinstance(obj, list) else [obj]
            for it in items:
                if not isinstance(it, dict):
                    continue
                t = it.get("@type") or it.get("type")
                if isinstance(t, list):
                    t = next((x for x in t if isinstance(x, str)), None)
                if t and str(t).lower() == "product":
                    data.update(it)
                    return data
        except Exception:  # noqa: BLE001
            continue
    return data


def _meta_content(soup: BeautifulSoup, name: str) -> Optional[str]:
    el = soup.find("meta", attrs={"property": name}) or soup.find("meta", attrs={"name": name})
    return (el.get("content") or "").strip() if el else None


def extract_product_signals(url: str) -> Dict[str, Any]:
    html = http_get(url) or ""
    soup = BeautifulSoup(html, "lxml")

    json_ld = _parse_json_ld(soup)
    title = (json_ld.get("name") if json_ld else None) or (soup.title.string if soup.title else None) or _meta_content(soup, "og:title") or ""
    title = clean_title(title)

    brand = None
    if json_ld:
        b = json_ld.get("brand")
        if isinstance(b, dict):
            brand = b.get("name")
        elif isinstance(b, str):
            brand = b

    description = None
    if json_ld:
        description = json_ld.get("description")
    if not description:
        description = _meta_content(soup, "og:description") or _meta_content(soup, "description")

    # Fallback common selectors for description
    if not description:
        desc_el = soup.select_one("#description, .product-description, .product__description, .productDesc, .pdp-description")
        if desc_el:
            description = desc_el.get_text(" ", strip=True)

    text_blob = " ".join(filter(None, [title, description or "", soup.get_text(" ")[:2000]]))
    ids = extract_identifiers(text_blob)

    h1 = None
    if not title:
        h1_el = soup.find("h1")
        h1 = h1_el.get_text(strip=True) if h1_el else None
        title = clean_title(h1 or "")

    # Attributes from JSON-LD and page structure
    attributes: Dict[str, str] = {}

    # 1) JSON-LD common fields
    if json_ld:
        for key in ["color", "size", "material", "pattern"]:
            val = json_ld.get(key)
            if isinstance(val, str) and val.strip():
                attributes[key] = val.strip()
        # additionalProperty: [{name, value}]
        addl = json_ld.get("additionalProperty")
        if isinstance(addl, list):
            for prop in addl:
                if not isinstance(prop, dict):
                    continue
                name = str(prop.get("name") or "").strip()
                value = str(prop.get("value") or "").strip()
                if not name or not value:
                    continue
                norm = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
                if norm and value and norm not in attributes:
                    attributes[norm] = value

    # 2) Spec tables and definition lists
    def _ingest_pair(k: str, v: str) -> None:
        if not k or not v:
            return
        k_norm = re.sub(r"[^a-z0-9]+", "_", k.lower()).strip("_")
        # simple synonym normalization
        k_norm = {
            "colour": "color",
            "screen": "screen_size",
            "display": "screen_size",
        }.get(k_norm, k_norm)
        if k_norm and k_norm not in attributes:
            attributes[k_norm] = v.strip()

    # tables
    for tbl in soup.select("table")[:6]:
        rows = tbl.select("tr")
        for row in rows[:30]:
            th = row.find("th")
            tds = row.find_all("td")
            if th and tds:
                key = th.get_text(" ", strip=True)
                val = tds[-1].get_text(" ", strip=True)
                _ingest_pair(key, val)

    # dl lists
    for dl in soup.select("dl")[:6]:
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            _ingest_pair(dt.get_text(" ", strip=True), dd.get_text(" ", strip=True))

    # bullet lists under spec/feature sections
    for sec in soup.find_all(True, attrs={"id": re.compile("spec|feature", re.I), "class": re.compile("spec|feature", re.I)}):
        for li in sec.select("li")[:30]:
            text = li.get_text(" ", strip=True)
            if ":" in text and len(text) < 200:
                k, v = text.split(":", 1)
                _ingest_pair(k, v)

    signals: Dict[str, Any] = {
        "url": url,
        "title": title,
        "brand": brand,
        "description": description or "",
        "identifiers": ids,
        "schema_present": bool(json_ld),
        "attributes": attributes,
    }

    # Try to guess model from title
    model = None
    m = re.search(r"\b([A-Z0-9]{3,}[-/][A-Z0-9\-]{2,})\b", title)
    if m:
        model = m.group(1)
    if model:
        signals["identifiers"].setdefault("model", model)

    return signals


