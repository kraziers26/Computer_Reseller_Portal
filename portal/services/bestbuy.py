"""
services/bestbuy.py

Best Buy fetcher for the ComputerReseller Portal.
Adapted from the iGamer Morning Report bot (bb_fetcher.py).
Key difference: uses synchronous `requests` instead of async aiohttp
so it works cleanly inside Flask routes and APScheduler jobs.

Scoring logic is identical to the Telegram bot — same Fresh Deal Score (0–13+).
"""

import os
import logging
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

BB_BASE  = "https://api.bestbuy.com/v1"
API_KEY  = os.environ.get("BESTBUY_API_KEY", "")

# ── Category map ──────────────────────────────────────────────────────────────
# Same category IDs as the Telegram bot.
# "slug" is used as the category value stored in bb_deals.category

CATEGORIES = [
    {"name": "Gaming Laptops",   "id": "pcmcat287600050003", "slug": "gaming"},
    {"name": "Gaming Desktops",  "id": "pcmcat287600050002", "slug": "desktop"},
    {"name": "MacBooks",         "id": "pcmcat247400050001", "slug": "macbook"},
    {"name": "Windows Laptops",  "id": "pcmcat247400050000", "slug": "laptop"},
    {"name": "All-in-One PCs",   "id": "abcat0501005",       "slug": "aio"},
]

CATEGORY_FALLBACKS = {
    "Gaming Laptops":  "categoryPath.name=Gaming Laptops&condition=new",
    "Gaming Desktops": "categoryPath.name=Gaming Desktops&condition=new",
}

# New, sealed only: Best Buy's condition attribute (New / Refurbished / Pre-Owned).
# Open-box is a separate buying option on the same SKU and never shows up here.
CONDITION_FILTER = "condition=new"

SHOW_FIELDS = ",".join([
    "sku", "name", "manufacturer", "salePrice", "regularPrice",
    "dollarSavings", "percentSavings", "onSale", "onlineAvailability",
    "url", "bestSellingRank", "priceUpdateDate", "offers",
    "shortDescription", "details"
])

POOL_SIZE       = 50
ALERT_THRESHOLD = 9
ALERT_MAX_HOURS = 6

EXCLUDE_WORDS = ("refurbished", "open-box", "open box", "pre-owned", "preowned", "renewed",
                 "geek squad certified")


# ── Helpers ───────────────────────────────────────────────────────────────────

def is_new(p: dict) -> bool:
    """Name-based safety net on top of condition=new in the query."""
    if (p.get("condition") or "new").strip().lower() != "new":
        return False
    return not any(w in (p.get("name") or "").lower() for w in EXCLUDE_WORDS)

def is_in_stock(p: dict) -> bool:
    return bool(p.get("onlineAvailability", True))


# ── Offer parsing ─────────────────────────────────────────────────────────────
# Copied directly from bb_fetcher.py — no changes.

def parse_offers(p: dict) -> dict:
    offers = p.get("offers") or []
    if not offers:
        return {"offer_type": None, "offer_label": "", "offer_note": "", "offer_score_bonus": 0}

    def norm(s):
        return (s or "").lower().strip()

    best_priority = 0
    best = {"offer_type": None, "offer_label": "", "offer_note": "", "offer_score_bonus": 0}

    for offer in offers:
        ot     = offer.get("offerType", "") or ""
        desc   = offer.get("description", "") or ""
        ot_n   = norm(ot)
        desc_n = norm(desc)

        if "deal of the day" in ot_n or "deal of the day" in desc_n:
            priority  = 5
            candidate = {
                "offer_type":        "Deal of the Day",
                "offer_label":       "DEAL OF THE DAY",
                "offer_note":        "Deal of the Day — valid until 11:59pm CT tonight",
                "offer_score_bonus": 4,
            }
        elif "clearance" in ot_n or "clearance" in desc_n:
            priority  = 4
            candidate = {
                "offer_type":        "Clearance",
                "offer_label":       "CLEARANCE",
                "offer_note":        desc or "Clearance item — limited units, no raincheck",
                "offer_score_bonus": 3,
            }
        elif "weekly ad" in ot_n or "weekly ad" in desc_n or "circular" in ot_n:
            priority  = 3
            candidate = {
                "offer_type":        "Weekly Ad",
                "offer_label":       "WEEKLY AD",
                "offer_note":        desc or "Weekly Ad — runs through Saturday",
                "offer_score_bonus": 2,
            }
        elif "special" in ot_n:
            priority  = 2
            candidate = {
                "offer_type":        "Special Offer",
                "offer_label":       "SPECIAL OFFER",
                "offer_note":        desc or "",
                "offer_score_bonus": 1,
            }
        elif ot_n or desc_n:
            priority  = 1
            candidate = {
                "offer_type":        ot or "Offer",
                "offer_label":       (ot or "OFFER").upper(),
                "offer_note":        desc or "",
                "offer_score_bonus": 1,
            }
        else:
            continue

        if priority > best_priority:
            best_priority = priority
            best = candidate

    return best


# ── Scoring ───────────────────────────────────────────────────────────────────
# Copied directly from bb_fetcher.py fresh_deal_score() — identical logic.

def fresh_deal_score(p: dict) -> int:
    score = 0

    price_date = p.get("priceUpdateDate")
    if price_date:
        try:
            dt   = datetime.fromisoformat(price_date.replace("Z", "+00:00"))
            now  = datetime.now(dt.tzinfo)
            days = (now - dt).days
            if days == 0:   score += 4
            elif days <= 2: score += 3
            elif days <= 7: score += 1
        except Exception:
            pass

    if p.get("onSale"):
        score += 2

    pct = float(p.get("percentSavings") or 0)
    if pct >= 20:   score += 3
    elif pct >= 10: score += 2
    elif pct >= 5:  score += 1

    save_d = float(p.get("dollarSavings") or 0)
    if save_d >= 300:   score += 2
    elif save_d >= 100: score += 1

    bs = p.get("bestSellingRank")
    if bs and bs <= 500:
        score += 1

    score += p.get("offer_score_bonus", 0)
    return score


def annotate_product(p: dict) -> dict:
    """Attach all derived fields. Mirrors bot's annotate_product."""
    offer_data = parse_offers(p)
    p.update(offer_data)
    p["fresh_score"] = fresh_deal_score(p)
    return p


# ── Spec extraction ───────────────────────────────────────────────────────────
# Extracts CPU and RAM from the BB API `details` array.
# The details array contains name/value pairs like:
#   {"name": "Processor", "value": "Intel Core i7-13700H"}
#   {"name": "RAM", "value": "16GB"}

PROCESSOR_KEYS = ("processor", "cpu", "processor model", "chipset")
MEMORY_KEYS    = ("memory", "ram", "system memory", "installed ram")

def extract_specs(p: dict) -> tuple:
    """Returns (cpu_str, memory_str) extracted from the details array."""
    details = p.get("details") or []
    cpu, memory = "", ""
    for d in details:
        key = (d.get("name") or "").lower().strip()
        val = (d.get("value") or "").strip()
        if not cpu and any(k in key for k in PROCESSOR_KEYS):
            cpu = val
        if not memory and any(k in key for k in MEMORY_KEYS):
            memory = val
        if cpu and memory:
            break
    return cpu, memory


# ── Core fetch (synchronous) ──────────────────────────────────────────────────

def _get(url: str, params: dict, timeout: int = 20) -> dict:
    """Thin wrapper around requests.get with error handling."""
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        logger.error(f"BB API HTTP error: {e} — {url}")
        return {}
    except Exception as e:
        logger.error(f"BB API request error: {e}")
        return {}


def fetch_category(cat: dict, filters: dict = None) -> list:
    """
    Fetch products for one category with optional filters.
    filters: {
        brands: list,
        price_min: float,
        price_max: float,
        cpu: list,        ← post-fetch filter (not in BB API)
        ram: list,        ← post-fetch filter (not in BB API)
        min_score: int
    }
    """
    filters = filters or {}

    query_parts = [f"categoryPath.id={cat['id']}", CONDITION_FILTER, "onSale=true"]
    if filters.get("brands"):
        brand_filter = " or ".join(f'manufacturer="{b}"' for b in filters["brands"])
        query_parts.append(f"({brand_filter})")
    if filters.get("price_min") is not None:
        query_parts.append(f"salePrice>={filters['price_min']}")
    if filters.get("price_max") is not None:
        query_parts.append(f"salePrice<{filters['price_max']}")

    url    = f"{BB_BASE}/products({'&'.join(query_parts)})"
    params = {
        "apiKey":   API_KEY,
        "format":   "json",
        "show":     SHOW_FIELDS,
        "sort":     "priceUpdateDate.dsc",
        "pageSize": str(POOL_SIZE),
    }

    data = _get(url, params)

    # Retry without onSale filter if empty (same pattern as bot)
    if not data.get("products"):
        query_parts2 = [p for p in query_parts if p != "onSale=true"]
        url2  = f"{BB_BASE}/products({'&'.join(query_parts2)})"
        data  = _get(url2, params)

    # Fallback for known categories
    if not data.get("products") and cat["name"] in CATEGORY_FALLBACKS:
        fallback_url = f"{BB_BASE}/products({CATEGORY_FALLBACKS[cat['name']]})"
        data = _get(fallback_url, params)

    products = [p for p in data.get("products", []) if is_new(p) and is_in_stock(p)]

    # Annotate
    for p in products:
        annotate_product(p)
        p["_category"]      = cat["name"]
        p["_category_slug"] = cat["slug"]

    # Post-fetch spec filters (CPU/RAM can't be queried via BB API)
    if filters.get("cpu"):
        cpu_filters = [c.lower() for c in filters["cpu"]]
        products = [
            p for p in products
            if any(cf in (p.get("shortDescription") or "").lower()
                   or any(cf in str(d.get("value","")).lower()
                          for d in (p.get("details") or []))
                   for cf in cpu_filters)
        ]

    if filters.get("ram"):
        products = [
            p for p in products
            if any(r.lower() in (p.get("shortDescription") or "").lower()
                   or any(r.lower() in str(d.get("value","")).lower()
                          for d in (p.get("details") or []))
                   for r in filters["ram"])
        ]

    if filters.get("min_score"):
        products = [p for p in products if p["fresh_score"] >= filters["min_score"]]

    products.sort(key=lambda p: p["fresh_score"], reverse=True)
    logger.info(f"  [{cat['name']}] {len(products)} qualifying products")
    return products


def run_scan(filters: dict = None) -> dict:
    """
    Main entry point called by routes and the scheduler.
    Fetches all (or filtered) categories and returns:
    {
        "products": [...],       ← flat list of all annotated products
        "categories_fetched": n,
        "total_raw": n,
    }
    filters: same shape as fetch_category filters dict.
    Can also include "categories": ["macbook", "gaming"] to limit which
    categories are fetched (matches slug values).
    """
    filters = filters or {}
    active_categories = CATEGORIES

    # Filter to specific category slugs if requested
    if filters.get("categories"):
        slugs = [s.lower() for s in filters["categories"]]
        active_categories = [c for c in CATEGORIES if c["slug"] in slugs]
        if not active_categories:
            active_categories = CATEGORIES

    all_products = []
    for cat in active_categories:
        try:
            prods = fetch_category(cat, filters)
            all_products.extend(prods)
        except Exception as e:
            logger.error(f"Scan error [{cat['name']}]: {e}")

    # Deduplicate by SKU (same product can appear in multiple categories)
    seen_skus = set()
    deduped   = []
    for p in all_products:
        sku = str(p.get("sku", ""))
        if sku and sku not in seen_skus:
            seen_skus.add(sku)
            deduped.append(p)

    deduped.sort(key=lambda p: p["fresh_score"], reverse=True)

    return {
        "products":           deduped,
        "categories_fetched": len(active_categories),
        "total_raw":          len(all_products),
    }


# ── DB upsert ─────────────────────────────────────────────────────────────────

def upsert_deals(conn, products: list) -> tuple:
    """
    Upsert a list of annotated products into bb_deals.
    Returns (deals_found, new_deals).
    Uses INSERT ... ON CONFLICT (sku) DO UPDATE to refresh price/score
    for existing SKUs and insert truly new ones.
    """
    if not products:
        return 0, 0

    deals_found = len(products)
    new_deals   = 0

    with conn.cursor() as cur:
        # Expire deals older than 48h before inserting fresh ones
        cur.execute("""
            UPDATE bb_deals
            SET is_active = FALSE
            WHERE expires_at < NOW() AND is_active = TRUE
        """)

        for p in products:
            cpu, memory = extract_specs(p)
            sku         = str(p.get("sku", ""))
            if not sku:
                continue

            # Check if this is a new SKU
            cur.execute("SELECT id FROM bb_deals WHERE sku = %s", (sku,))
            existing = cur.fetchone()

            cur.execute("""
                INSERT INTO bb_deals (
                    sku, name, brand, category,
                    sale_price, regular_price, discount_pct, score,
                    cpu, memory, url,
                    fetched_at, expires_at, is_active
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    NOW(), NOW() + INTERVAL '48 hours', TRUE
                )
                ON CONFLICT (sku) DO UPDATE SET
                    name          = EXCLUDED.name,
                    brand         = EXCLUDED.brand,
                    category      = EXCLUDED.category,
                    sale_price    = EXCLUDED.sale_price,
                    regular_price = EXCLUDED.regular_price,
                    discount_pct  = EXCLUDED.discount_pct,
                    score         = EXCLUDED.score,
                    cpu           = EXCLUDED.cpu,
                    memory        = EXCLUDED.memory,
                    url           = EXCLUDED.url,
                    fetched_at    = NOW(),
                    expires_at    = NOW() + INTERVAL '48 hours',
                    is_active     = TRUE
            """, (
                sku,
                (p.get("name") or "")[:255],
                (p.get("manufacturer") or "")[:100],
                p.get("_category_slug", "")[:100],
                float(p.get("salePrice") or 0),
                float(p.get("regularPrice") or 0),
                int(float(p.get("percentSavings") or 0)),
                int(p.get("fresh_score", 0)),
                cpu[:150] if cpu else None,
                memory[:50] if memory else None,
                (p.get("url") or "")[:2000],
            ))

            if not existing:
                new_deals += 1

        conn.commit()

    logger.info(f"Upserted {deals_found} deals ({new_deals} new)")
    return deals_found, new_deals


# ── Price-drop engine fetchers ────────────────────────────────────────────────
# Used by services/price_engine.py. Unlike fetch_category() above, these:
#   • raise BBApiError instead of returning {} (so a failed sync is visible)
#   • page through every result (pageSize 100)
#   • don't require onSale=true (a drop can happen on a non-sale item)
#   • count API calls

import time as _time
import re as _re

PRICE_FIELDS = ",".join([
    "sku", "name", "manufacturer", "salePrice", "regularPrice", "onSale",
    "percentSavings", "dollarSavings", "onlineAvailability", "orderable",
    "url", "image", "priceUpdateDate", "condition", "quantityLimit",
    "modelNumber", "upc",
])

PAGE_SIZE       = 100
MAX_PAGES       = 30      # safety cap per query (3,000 products)
# Pause after every call. Best Buy enforces a per-second limit per API key
# (shared with anything else using the same key, e.g. the morning-report bot).
CALL_SPACING_S  = float(os.environ.get("BESTBUY_CALL_SPACING_S", "1.0"))
RATE_LIMIT_WAITS = (2, 4, 8, 16, 30)   # back-off when Best Buy says "per second limit"


class BBApiError(Exception):
    pass


class CallCounter:
    def __init__(self):
        self.calls = 0


def _is_rate_limited(resp) -> bool:
    """Best Buy answers 429 *or* 403 + "per second limit" when throttling."""
    if resp.status_code == 429:
        return True
    if resp.status_code == 403:
        body = (resp.text or "").lower()
        return "per second" in body or "rate limit" in body
    return False


def _get_strict(url: str, params: dict, counter: CallCounter = None, timeout: int = 25) -> dict:
    if not API_KEY:
        raise BBApiError("BESTBUY_API_KEY is not set")
    params = dict(params, apiKey=API_KEY, format="json")
    last_err = None
    net_failures = 0
    for attempt in range(len(RATE_LIMIT_WAITS) + 1):
        if counter is not None:
            counter.calls += 1
        try:
            resp = requests.get(url, params=params, timeout=timeout)
        except Exception as e:
            net_failures += 1
            last_err = f"request error: {e}"
            if net_failures >= 3:
                break
            _time.sleep(1.5 * net_failures)
            continue
        if _is_rate_limited(resp):
            last_err = f"Best Buy per-second limit (HTTP {resp.status_code}) — still limited after {attempt + 1} tries"
            if attempt < len(RATE_LIMIT_WAITS):
                logger.warning(f"BB API rate limited, waiting {RATE_LIMIT_WAITS[attempt]}s")
                _time.sleep(RATE_LIMIT_WAITS[attempt])
            continue
        if resp.status_code != 200:
            raise BBApiError(f"HTTP {resp.status_code}: {resp.text[:200]}")
        _time.sleep(CALL_SPACING_S)
        return resp.json()
    raise BBApiError(last_err or "unknown error")


def query_products(query: str, counter: CallCounter = None, sort: str = None,
                   max_pages: int = MAX_PAGES) -> list:
    """All products matching a Best Buy query string, e.g.
    'categoryPath.id=abc&condition=new'. Pages until done."""
    url = f"{BB_BASE}/products({query})"
    out = []
    page = 1
    while page <= max_pages:
        params = {"show": PRICE_FIELDS, "pageSize": str(PAGE_SIZE), "page": str(page)}
        if sort:
            params["sort"] = sort
        data = _get_strict(url, params, counter)
        out.extend(data.get("products") or [])
        total_pages = int(data.get("totalPages") or 1)
        if page >= total_pages:
            break
        page += 1
    return out


def bb_date_param(dt) -> str:
    """Best Buy query dates look like 2026-10-07T16:30:00 (no timezone)."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S")


def fetch_category_all(cat: dict, counter: CallCounter = None) -> list:
    """Full sweep: every new product in the category, on sale or not."""
    prods = query_products(f"categoryPath.id={cat['id']}&{CONDITION_FILTER}", counter)
    return [p for p in prods if is_new(p)]


def fetch_category_changed_since(cat: dict, since_dt, counter: CallCounter = None) -> list:
    """Delta: only products whose price changed after since_dt.
    The caller passes a generous window; change detection is done against
    our own stored prices, so overlap is harmless."""
    q = (f"categoryPath.id={cat['id']}&{CONDITION_FILTER}"
         f"&priceUpdateDate>{bb_date_param(since_dt)}")
    prods = query_products(q, counter, sort="priceUpdateDate.dsc")
    return [p for p in prods if is_new(p)]


def fetch_skus(skus: list, counter: CallCounter = None) -> list:
    """Current state of specific SKUs (watched items), 100 per call."""
    skus = [str(s) for s in skus if str(s).isdigit()]
    out = []
    for i in range(0, len(skus), PAGE_SIZE):
        chunk = ",".join(skus[i:i + PAGE_SIZE])
        out.extend(query_products(f"sku in({chunk})", counter, max_pages=1))
    return out


def _clean_term(s: str) -> str:
    # Best Buy query values can't contain these characters
    return _re.sub(r"[&()=<>,\"*]", " ", s or "").strip()


def lookup_by_upc(upc: str, counter: CallCounter = None) -> list:
    upc = _re.sub(r"\D", "", upc or "")
    if not upc:
        return []
    return query_products(f"upc={upc}", counter, max_pages=1)


def lookup_by_model(model: str, counter: CallCounter = None) -> list:
    model = _clean_term(model)
    if not model:
        return []
    found = query_products(f"modelNumber={model}", counter, max_pages=1)
    if not found:
        # Fall back to keyword search on the model text
        terms = "&".join(f"search={t}" for t in model.split()[:6])
        found = query_products(terms, counter, max_pages=1)
    return found


def fetch_raw_product(sku: str) -> dict:
    """Every field Best Buy returns for one SKU (no 'show' filter).
    Used by the diagnostics route to check which seller/marketplace
    fields exist before relying on them."""
    data = _get_strict(f"{BB_BASE}/products(sku={int(sku)})", {})
    prods = data.get("products") or []
    return prods[0] if prods else {}


# ── Connection test ───────────────────────────────────────────────────────────

def test_connection() -> tuple:
    """Quick health check — returns (ok: bool, message: str)"""
    url    = f"{BB_BASE}/products(search=laptop)"
    params = {
        "apiKey":   API_KEY,
        "format":   "json",
        "show":     "sku,name,salePrice",
        "pageSize": "3",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 403:
            return False, "API key invalid or rate limited (403)"
        if resp.status_code != 200:
            return False, f"HTTP {resp.status_code}"
        data = resp.json()
        count = len(data.get("products", []))
        return True, f"Connected — {count} test products returned"
    except Exception as e:
        return False, f"Connection error: {e}"
