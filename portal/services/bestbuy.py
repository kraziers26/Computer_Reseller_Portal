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
    "Gaming Laptops":  "categoryPath.name=Gaming Laptops",
    "Gaming Desktops": "categoryPath.name=Gaming Desktops",
}

SHOW_FIELDS = ",".join([
    "sku", "name", "manufacturer", "salePrice", "regularPrice",
    "dollarSavings", "percentSavings", "onSale", "onlineAvailability",
    "url", "bestSellingRank", "priceUpdateDate", "offers",
    "shortDescription", "details"
])

POOL_SIZE       = 50
ALERT_THRESHOLD = 9
ALERT_MAX_HOURS = 6

EXCLUDE_WORDS = ("refurbished", "open-box", "open box", "pre-owned", "preowned", "renewed")


# ── Helpers ───────────────────────────────────────────────────────────────────

def is_new(p: dict) -> bool:
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

    query_parts = [f"categoryPath.id={cat['id']}", "onSale=true"]
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
