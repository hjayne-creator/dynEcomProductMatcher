import math
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

from rapidfuzz import fuzz
from openai import OpenAI

from .extractor import extract_product_signals
from .utils import get_domain


_openai_client = None


def _get_client() -> OpenAI:
    global _openai_client
    if _openai_client is None:
        _openai_client = OpenAI()
    return _openai_client


def _embed(texts: List[str]) -> List[List[float]]:
    if not texts:
        return []
    client = _get_client()
    resp = client.embeddings.create(
        model="text-embedding-3-small",
        input=texts,
    )
    return [d.embedding for d in resp.data]


def _cosine(a: List[float], b: List[float]) -> float:
    if not a or not b:
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    na = math.sqrt(sum(x * x for x in a))
    nb = math.sqrt(sum(x * x for x in b))
    if na == 0 or nb == 0:
        return 0.0
    return dot / (na * nb)


def _compute_similarity(seed: Dict, cand: Dict, seed_emb: List[float], cand_emb: List[float]) -> float:
    title_seed = (seed.get("title") or "").lower()
    title_cand = (cand.get("title") or "").lower()
    brand_seed = (seed.get("brand") or "").lower()
    brand_cand = (cand.get("brand") or "").lower()
    ids_seed = seed.get("identifiers") or {}
    ids_cand = cand.get("identifiers") or {}
    attrs_seed = seed.get("attributes") or {}
    attrs_cand = cand.get("attributes") or {}

    # Embedding similarity
    s_emb = _cosine(seed_emb, cand_emb)

    # Fuzzy title match
    s_title = fuzz.token_set_ratio(title_seed, title_cand) / 100.0

    # Identifier overlap
    overlap = 0.0
    for key in ["gtin", "mpn", "sku", "model"]:
        if ids_seed.get(key) and ids_cand.get(key) and ids_seed.get(key) == ids_cand.get(key):
            overlap += 1.0
    s_id = min(overlap, 2.0) / 2.0  # cap

    # Brand match
    s_brand = 1.0 if brand_seed and brand_seed == brand_cand else 0.0

    # Attribute overlap (normalized over min number of attrs)
    def _normalize_value(val: str) -> str:
        return " ".join(str(val).lower().split())

    def _numbers(s: str) -> List[str]:
        import re as _re  # local import to avoid top-level extra dep
        return _re.findall(r"\d+(?:\.\d+)?", s or "")

    def _values_match(a: str, b: str) -> bool:
        if not a or not b:
            return False
        na, nb = _numbers(a), _numbers(b)
        if na and nb and any(x in nb for x in na):
            return True
        return fuzz.token_set_ratio(_normalize_value(a), _normalize_value(b)) >= 85

    common_keys = set(attrs_seed.keys()) & set(attrs_cand.keys())
    matches = 0
    for k in list(common_keys)[:12]:  # cap for speed
        if _values_match(str(attrs_seed.get(k, "")), str(attrs_cand.get(k, ""))):
            matches += 1
    denom = max(1, min(len(attrs_seed), len(attrs_cand)))
    s_attr = matches / denom

    # Weights include attributes
    similarity = 0.40 * s_emb + 0.22 * s_title + 0.18 * s_id + 0.10 * s_brand + 0.10 * s_attr
    return similarity


def score_candidates(seed_signals: Dict, candidate_urls: List[str]) -> List[Dict]:
    # Extract candidate signals concurrently
    candidates: List[Dict] = []
    with ThreadPoolExecutor(max_workers=6) as ex:
        futures = {ex.submit(extract_product_signals, url): url for url in candidate_urls}
        for fut in as_completed(futures):
            try:
                sig = fut.result()
                candidates.append(sig)
            except Exception:  # noqa: BLE001
                continue

    # Prepare embeddings
    seed_attrs_str = " ".join(
        f"{k}:{v}" for k, v in list((seed_signals.get("attributes") or {}).items())[:8]
    )
    seed_text = " ".join(
        [
            seed_signals.get("title") or "",
            seed_signals.get("brand") or "",
            " ".join((seed_signals.get("identifiers") or {}).values()),
            seed_attrs_str,
        ]
    )
    cand_texts = []
    for c in candidates:
        c_attrs_str = " ".join(f"{k}:{v}" for k, v in list((c.get("attributes") or {}).items())[:8])
        cand_texts.append(
            " ".join([
                c.get("title") or "",
                c.get("brand") or "",
                " ".join((c.get("identifiers") or {}).values()),
                c_attrs_str,
            ])
        )
    embs = _embed([seed_text] + cand_texts)
    seed_emb = embs[0] if embs else []
    cand_embs = embs[1:] if len(embs) > 1 else [[] for _ in candidates]

    scored = []
    for cand, emb in zip(candidates, cand_embs):
        sim = _compute_similarity(seed_signals, cand, seed_emb, emb)
        scored.append(
            {
                "domain": get_domain(cand.get("url", "")),
                "url": cand.get("url"),
                "similarity": float(sim),
                "signals": cand,
            }
        )

    # Keep top 3–5 above threshold
    scored = sorted(scored, key=lambda x: x["similarity"], reverse=True)
    scored = [s for s in scored if s["similarity"] >= 0.50][:5]
    return scored


