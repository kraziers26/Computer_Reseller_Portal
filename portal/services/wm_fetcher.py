"""
services/wm_fetcher.py

Synchronous Walmart scraper for the ComputerReseller Portal.
Uses requests.Session with Akamai-bypass headers + homepage warm-up.
Extracts __NEXT_DATA__ JSON embedded in Walmart search HTML pages.

Categories mirrored from the Telegram bot:
  Gaming Desktops, Gaming Laptops, MacBooks, All-in-One PCs, Windows Laptops
"""

import re
import json
import time
import logging
import requests
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

# ── Categories ────────────────────────────────────────────────────────────────

CATEGORIES = {
    "gaming_laptops":  {"name": "Gaming Laptops",   "query": "gaming laptop",        "sort": "price_low"},
    "gaming_desktops": {"name": "Gaming Desktops",  "query": "gaming desktop pc",    "sort": "price_low"},
    "macbooks":        {"name": "MacBooks",          "query": "apple macbook",        "sort": "price_low"},
    "laptops":         {"name": "Windows Laptops",  "query": "windows laptop",       "sort": "price_low"},
    "aio":             {"name": "All-in-One PCs",   "query": "all in one desktop pc","sort": "price_low"},
}

# ── Scoring ───────────────────────────────────────────────────────────────────

def _fresh_deal_score(item: dict) -> int:
    """
    Fresh Deal Score (0–13) — same logic as the Telegram bot.
    Rewards deep discounts and recently changed prices.
    """
    score = 0
    discount = item.get("discount_pct", 0) or 0

    # Discount depth (0–8 pts)
    if discount >= 40: score += 8
    elif discount >= 30: score += 6
    elif discount >= 20: score += 4
    elif discount >= 15: score += 3
    elif discount >= 10: score += 2
    elif discount >= 5:  score += 1

    # Price bracket (0–3 pts) — sweet spot for reseller margin
    price = item.get("sale_price", 0) or 0
    if 300 <= price <= 800:   score += 3
    elif 800 < price <= 1500: score += 2
    elif price > 1500:        score += 1

    # Availability (0–2 pts)
    if item.get("in_stock"):   score += 2

    return min(score, 13)


# ── Header set ────────────────────────────────────────────────────────────────

def _build_headers(referer: str = "https://www.walmart.com/") -> dict:
    return {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": referer,
        "Origin": "https://www.walmart.com",
        "Sec-CH-UA": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "Sec-CH-UA-Mobile": "?0",
        "Sec-CH-UA-Platform": '"Windows"',
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "Connection": "keep-alive",
    }


# ── Session warm-up ───────────────────────────────────────────────────────────

def _warm_session(session: requests.Session) -> bool:
    """
    Visit Walmart homepage to collect Akamai cookies (ak_bmsc, bm_sv).
    Without this, search requests return HTTP 412.
    """
    try:
        resp = session.get(
            "https://www.walmart.com/",
            headers=_build_headers("https://www.google.com/"),
            timeout=15,
            allow_redirects=True,
        )
        logger.debug(f"[WM warm-up] Homepage: {resp.status_code}")
        time.sleep(1.5)  # Mimic human pause

        # Hit a search page so Akamai sees a natural browsing pattern
        resp2 = session.get(
            "https://www.walmart.com/search?q=laptop&sort=price_low",
            headers=_build_headers("https://www.walmart.com/"),
            timeout=15,
        )
        logger.debug(f"[WM warm-up] Search page: {resp2.status_code}")
        time.sleep(1.0)

        return resp.status_code == 200
    except Exception as e:
        logger.warning(f"[WM warm-up] Failed: {e}")
        return False


# ── __NEXT_DATA__ extraction ──────────────────────────────────────────────────

def _extract_next_data(html: str) -> Optional[dict]:
    """Extract the embedded __NEXT_DATA__ JSON from a Walmart search page."""
    m = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except json.JSONDecodeError as e:
        logger.warning(f"[WM] __NEXT_DATA__ JSON parse error: {e}")
        return None


def _find_items_in_next_data(data: dict) -> list:
    """
    Walk the __NEXT_DATA__ tree to find product item arrays.
    Walmart's structure changes periodically; this tries multiple known paths.
    """
    items = []

    def _walk(obj, depth=0):
        if depth > 12 or not isinstance(obj, (dict, list)):
            return
        if isinstance(obj, list):
            # A list of dicts that all have "usItemId" is a product list
            if len(obj) > 0 and isinstance(obj[0], dict) and "usItemId" in obj[0]:
                items.extend(obj)
                return
            for v in obj:
                _walk(v, depth + 1)
        elif isinstance(obj, dict):
            # Shortcut: known container keys
            for key in ("items", "products", "searchResult", "initialData",
                        "searchContent", "paginatedItems", "itemStacks"):
                if key in obj:
                    _walk(obj[key], depth + 1)
            # Also walk all values
            for v in obj.values():
                _walk(v, depth + 1)

    _walk(data)
    return items


# ── Item normalisation ────────────────────────────────────────────────────────

def _normalise(raw: dict, category_key: str) -> Optional[dict]:
    """Convert a raw Walmart product dict into a portal-ready dict."""
    try:
        name = raw.get("name") or raw.get("title") or ""
        if not name:
            return None

        # Price
        price_info = raw.get("priceInfo") or raw.get("price") or {}
        if isinstance(price_info, (int, float)):
            sale_price = float(price_info)
            reg_price  = sale_price
        else:
            sale_price = float(
                price_info.get("currentPrice", {}).get("price", 0)
                or price_info.get("linePrice", 0)
                or price_info.get("price", 0)
                or 0
            )
            reg_price = float(
                price_info.get("wasPrice", {}).get("price", 0)
                or price_info.get("comparisonPrice", {}).get("price", 0)
                or price_info.get("listPrice", 0)
                or sale_price
            )

        if sale_price <= 0:
            return None

        discount_pct = round((reg_price - sale_price) / reg_price * 100, 1) if reg_price > sale_price else 0.0

        # Stock
        avail = raw.get("availabilityStatus") or raw.get("availability") or ""
        in_stock = "IN_STOCK" in avail.upper() if avail else True

        # Brand
        brand = raw.get("brand") or raw.get("manufacturerName") or ""

        # URL
        item_id  = raw.get("usItemId") or raw.get("itemId") or ""
        product_url = raw.get("canonicalUrl") or f"/ip/{item_id}"
        if not product_url.startswith("http"):
            product_url = "https://www.walmart.com" + product_url

        item = {
            "item_id":      str(item_id),
            "name":         name[:300],
            "brand":        str(brand)[:60] if brand else "",
            "category":     category_key,
            "sale_price":   sale_price,
            "reg_price":    reg_price,
            "discount_pct": discount_pct,
            "in_stock":     in_stock,
            "url":          product_url,
            "fetched_at":   datetime.utcnow(),
        }
        item["score"] = _fresh_deal_score(item)
        return item

    except Exception as e:
        logger.debug(f"[WM normalise] Skipped item: {e}")
        return None


# ── Category fetch ────────────────────────────────────────────────────────────

def _fetch_category(session: requests.Session, cat_key: str, cat_cfg: dict, max_pages: int = 2) -> list:
    """Fetch up to max_pages of results for one category."""
    results = []
    query   = cat_cfg["query"]

    for page in range(1, max_pages + 1):
        url = (
            f"https://www.walmart.com/search"
            f"?q={requests.utils.quote(query)}"
            f"&sort=price_low"
            f"&page={page}"
            f"&affinityOverride=default"
        )
        try:
            resp = session.get(
                url,
                headers=_build_headers("https://www.walmart.com/"),
                timeout=20,
            )
            logger.debug(f"[WM] {cat_key} page {page}: HTTP {resp.status_code}")

            if resp.status_code == 412:
                logger.warning(f"[WM] 412 on {cat_key} page {page} — Akamai block")
                break
            if resp.status_code != 200:
                logger.warning(f"[WM] HTTP {resp.status_code} on {cat_key} page {page}")
                break

            next_data = _extract_next_data(resp.text)
            if not next_data:
                logger.warning(f"[WM] No __NEXT_DATA__ on {cat_key} page {page}")
                break

            raw_items = _find_items_in_next_data(next_data)
            logger.debug(f"[WM] {cat_key} page {page}: found {len(raw_items)} raw items")

            page_results = []
            for raw in raw_items:
                normed = _normalise(raw, cat_key)
                if normed and normed["discount_pct"] >= 5:  # Only show discounted items
                    page_results.append(normed)

            results.extend(page_results)

            if not page_results:
                break  # No results on this page, stop paginating

            time.sleep(0.8)  # Polite delay between pages

        except requests.Timeout:
            logger.warning(f"[WM] Timeout on {cat_key} page {page}")
            break
        except Exception as e:
            logger.error(f"[WM] Error on {cat_key} page {page}: {e}")
            break

    return results


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_walmart_deals(categories: Optional[list] = None, min_discount: float = 5.0) -> dict:
    """
    Main entry point. Fetches deals from Walmart for the specified categories.

    Args:
        categories: list of category keys (e.g. ["gaming_laptops", "macbooks"]).
                    If None, fetches all categories.
        min_discount: minimum discount % to include (default 5%).

    Returns:
        {
            "ok": True/False,
            "items": [...],
            "total": int,
            "categories_fetched": int,
            "error": str or None,
            "fetched_at": datetime,
        }
    """
    cats_to_fetch = {
        k: v for k, v in CATEGORIES.items()
        if categories is None or k in categories
    }

    session = requests.Session()
    session.headers.update({"Accept-Encoding": "gzip, deflate, br"})

    # Warm up the session to get Akamai cookies
    warmed = _warm_session(session)
    if not warmed:
        logger.warning("[WM] Session warm-up failed — proceeding anyway")

    all_items = []
    cats_ok   = 0

    for cat_key, cat_cfg in cats_to_fetch.items():
        try:
            items = _fetch_category(session, cat_key, cat_cfg)
            if items:
                cats_ok += 1
                # Filter by min_discount
                filtered = [i for i in items if i["discount_pct"] >= min_discount]
                all_items.extend(filtered)
                logger.info(f"[WM] {cat_cfg['name']}: {len(filtered)} deals (min {min_discount}% off)")
            time.sleep(1.2)  # Pause between categories
        except Exception as e:
            logger.error(f"[WM] Category {cat_key} failed: {e}")

    # Deduplicate by item_id
    seen     = set()
    deduped  = []
    for item in all_items:
        if item["item_id"] not in seen:
            seen.add(item["item_id"])
            deduped.append(item)

    # Sort by score desc
    deduped.sort(key=lambda x: x["score"], reverse=True)

    return {
        "ok":                 cats_ok > 0,
        "items":              deduped,
        "total":              len(deduped),
        "categories_fetched": cats_ok,
        "error":              None if cats_ok > 0 else "No categories returned data",
        "fetched_at":         datetime.utcnow(),
    }
