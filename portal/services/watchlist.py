"""
services/watchlist.py

Watchlist: paste a link (any retailer), a model number or a UPC →
find the matching NEW Best Buy SKU → confirm → track it every sync.

Round 1 tracks the Best Buy listing. A link from another retailer is kept
as a 'linked' listing (shown as "coming soon") so Amazon / Walmart can be
switched on later without changing the data.
"""

import json
import logging
import re
import secrets
from urllib.parse import urlparse, parse_qs

import requests

from . import bestbuy
from .price_engine import process_products, now_utc, in_stock, live_history_days

logger = logging.getLogger(__name__)

RETAILERS = {
    "bestbuy.com": "bestbuy", "amazon.com": "amazon", "amzn.to": "amazon", "a.co": "amazon",
    "walmart.com": "walmart", "newegg.com": "newegg", "costco.com": "costco",
    "bhphotovideo.com": "bhphoto", "microcenter.com": "microcenter", "target.com": "target",
}
RETAILER_LABELS = {"bestbuy": "Best Buy", "amazon": "Amazon", "walmart": "Walmart", "newegg": "Newegg",
                   "costco": "Costco", "bhphoto": "B&H", "microcenter": "Micro Center",
                   "target": "Target", "other": "Other"}


class WatchError(Exception):
    pass


def retailer_of(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    for dom, key in RETAILERS.items():
        if host == dom or host.endswith("." + dom):
            return key
    return "other"


def parse_input(text: str) -> dict:
    t = (text or "").strip()
    if not t:
        raise WatchError("Paste a link, a model number or a UPC.")
    if re.match(r"^https?://", t, re.I):
        r = retailer_of(t)
        if r == "bestbuy":
            q = parse_qs(urlparse(t).query)
            sku = (q.get("skuId") or [None])[0]
            if not sku:
                m = re.search(r"/(\d{6,8})\.p", t) or re.search(r"/sku/(\d{6,8})", t) \
                    or re.search(r"/(\d{6,8})(?:[/?#]|$)", t)
                sku = m.group(1) if m else None
            if sku:
                return {"kind": "bb_sku", "sku": sku, "url": t}
        return {"kind": "url", "retailer": r, "url": t}
    digits = re.sub(r"\D", "", t)
    if re.fullmatch(r"\d{6,8}", t):
        return {"kind": "bb_sku", "sku": t}
    if re.fullmatch(r"[\d\s-]{11,15}", t) and len(digits) in (12, 13, 14):
        return {"kind": "upc", "upc": digits}
    return {"kind": "model", "model": t}


def _ids_from_page(url: str) -> dict:
    """Best effort: read model number / UPC from a retailer page's structured
    data. Many retailers block servers (Railway IPs), so this often fails —
    the UI then asks for the model number or UPC instead."""
    try:
        r = requests.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"})
        if r.status_code != 200:
            return {}
        page = r.text[:3_000_000]
    except Exception:
        return {}
    found = {}
    for block in re.findall(r'<script[^>]+application/ld\+json[^>]*>(.*?)</script>', page, re.S | re.I):
        try:
            data = json.loads(block.strip())
        except Exception:
            continue
        stack = data if isinstance(data, list) else [data]
        while stack:
            d = stack.pop()
            if isinstance(d, dict):
                for k in ("gtin12", "gtin13", "gtin14", "gtin"):
                    if d.get(k) and "upc" not in found:
                        found["upc"] = re.sub(r"\D", "", str(d[k]))
                if d.get("mpn") and "model" not in found:
                    found["model"] = str(d["mpn"]).strip()
                if d.get("name") and "name" not in found and d.get("@type") == "Product":
                    found["name"] = str(d["name"])[:200]
                stack.extend(v for v in d.values() if isinstance(v, (dict, list)))
            elif isinstance(d, list):
                stack.extend(d)
    if "model" not in found:
        m = re.search(r'"(?:modelNumber|model_number|mpn)"\s*:\s*"([^"]{3,40})"', page)
        if m:
            found["model"] = m.group(1)
    return found


def _candidate(p: dict) -> dict:
    condition = (p.get("condition") or "New")
    ok = bestbuy.is_new(p)
    online = p.get("onlineAvailability")
    return {
        "sku": str(p.get("sku")), "name": p.get("name"), "price": p.get("salePrice"),
        "regular_price": p.get("regularPrice"), "model_number": p.get("modelNumber"),
        "upc": p.get("upc"), "url": p.get("url"), "image_url": p.get("image"),
        "condition": condition,
        "in_stock": in_stock(p.get("orderable"), None if online is None else bool(online)),
        "eligible": ok,
        "reason": None if ok else f"{condition} listing — the watchlist only tracks new, sealed items.",
    }


def match(text: str) -> dict:
    """Returns {source, other_listing, candidates:[...], message}"""
    parsed = parse_input(text)
    other = None
    candidates, message = [], None

    if parsed["kind"] == "bb_sku":
        candidates = bestbuy.fetch_skus([parsed["sku"]])
        source = "Best Buy link" if parsed.get("url") else "Best Buy SKU"
    elif parsed["kind"] == "upc":
        candidates = bestbuy.lookup_by_upc(parsed["upc"])
        source = "UPC"
    elif parsed["kind"] == "model":
        candidates = bestbuy.lookup_by_model(parsed["model"])
        source = "model number"
    else:
        r = parsed["retailer"]
        other = {"retailer": r, "label": RETAILER_LABELS.get(r, "Other"), "url": parsed["url"]}
        ids = _ids_from_page(parsed["url"])
        if ids.get("upc"):
            candidates = bestbuy.lookup_by_upc(ids["upc"])
            source = f"UPC from your {other['label']} link"
        if not candidates and ids.get("model"):
            candidates = bestbuy.lookup_by_model(ids["model"])
            source = f"model number from your {other['label']} link"
        if not ids:
            source = f"{other['label']} link"
            message = (f"Couldn't read the product details from that {other['label']} page "
                       f"(retailers often block servers). Paste the model number or UPC instead — "
                       f"the link will still be saved with the watch.")
        elif not candidates:
            source = f"{other['label']} link"
            message = "Best Buy doesn't seem to carry that model as a new item."

    cands = [_candidate(p) for p in candidates][:8]
    if not cands and not message:
        message = "No Best Buy match found. Try the exact model number or the UPC."
    return {"source": source, "parsed": parsed["kind"], "other_listing": other,
            "candidates": cands, "message": message}


def create_watch(conn, sku: str, target_price=None, other_url=None, rules=None, user_id=None) -> int:
    prods = bestbuy.fetch_skus([sku])
    if not prods:
        raise WatchError("That Best Buy SKU wasn't found.")
    p = prods[0]
    if not bestbuy.is_new(p):
        raise WatchError("That listing isn't new/sealed. The watchlist only tracks new items sold new.")

    rules = rules or {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT w.id FROM watch_products w JOIN watch_listings l ON l.product_id = w.id
            WHERE w.is_active AND l.retailer = 'bestbuy' AND l.retailer_ref = %s
        """, (str(p["sku"]),))
        if cur.fetchone():
            raise WatchError("You're already watching this item.")

    # Make sure history starts now (adds bb_products row on first sight)
    process_products(conn, [p], category="watch")

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO watch_products (name, model_number, upc, target_price,
                alert_drop, alert_below_target, alert_all_time_low, alert_back_in_stock, alert_price_up,
                price_when_added, action_token, created_by)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
        """, ((p.get("name") or "")[:255], p.get("modelNumber"), str(p.get("upc") or "") or None,
              _money(target_price),
              rules.get("drop", True), rules.get("below_target", True), rules.get("all_time_low", True),
              rules.get("back_in_stock", True), rules.get("price_up", False),
              p.get("salePrice"), secrets.token_urlsafe(24), user_id))
        wid = cur.fetchone()["id"]
        cur.execute("""INSERT INTO watch_listings (product_id, retailer, retailer_ref, url, status)
                       VALUES (%s, 'bestbuy', %s, %s, 'tracked')""", (wid, str(p["sku"]), p.get("url")))
        if other_url:
            r = retailer_of(other_url)
            if r != "bestbuy":
                cur.execute("""INSERT INTO watch_listings (product_id, retailer, url, status)
                               VALUES (%s, %s, %s, 'linked')""", (wid, r, other_url[:2000]))
    conn.commit()
    return wid


def _money(v):
    if v in (None, ""):
        return None
    try:
        v = round(float(str(v).replace("$", "").replace(",", "")), 2)
        return v if v > 0 else None
    except Exception:
        raise WatchError("Target price must be a number.")


def update_watch(conn, wid: int, data: dict):
    sets, params = [], []
    if "target_price" in data:
        sets.append("target_price = %s"); params.append(_money(data["target_price"]))
    for k in ("alert_drop", "alert_below_target", "alert_all_time_low", "alert_back_in_stock", "alert_price_up"):
        if k in data:
            sets.append(f"{k} = %s"); params.append(bool(data[k]))
    if "snooze_hours" in data:
        h = int(data["snooze_hours"] or 0)
        sets.append("snoozed_until = CASE WHEN %s > 0 THEN NOW() + make_interval(hours => %s) ELSE NULL END")
        params += [h, h]
    if not sets:
        return
    with conn.cursor() as cur:
        cur.execute(f"UPDATE watch_products SET {', '.join(sets)} WHERE id = %s", params + [wid])
    conn.commit()


def stop_watch(conn, wid: int):
    with conn.cursor() as cur:
        cur.execute("UPDATE watch_products SET is_active = FALSE WHERE id = %s", (wid,))
    conn.commit()


def list_watches(conn) -> list:
    now = now_utc()
    with conn.cursor() as cur:
        cur.execute("""SELECT * FROM watch_products WHERE is_active ORDER BY created_at DESC""")
        ws = cur.fetchall()
        if not ws:
            return []
        cur.execute("SELECT * FROM watch_listings WHERE product_id = ANY(%s) ORDER BY id",
                    ([w["id"] for w in ws],))
        listings = cur.fetchall()
        skus = [l["retailer_ref"] for l in listings if l["retailer"] == "bestbuy"]
        cur.execute("""SELECT p.*, s.all_time_low, s.history_days, s.updated_at AS stats_updated_at FROM bb_products p
                       LEFT JOIN bb_sku_stats s ON s.sku = p.sku WHERE p.sku = ANY(%s)""", (skus,))
        prods = {p["sku"]: p for p in cur.fetchall()}
        cur.execute("""SELECT DISTINCT ON (product_id) product_id, created_at, status FROM alert_log
                       WHERE kind = 'watch' AND product_id = ANY(%s)
                       ORDER BY product_id, created_at DESC""", ([w["id"] for w in ws],))
        last_alert = {a["product_id"]: a for a in cur.fetchall()}

    out = []
    for w in ws:
        ls = []
        best = None
        for l in [x for x in listings if x["product_id"] == w["id"]]:
            item = {"retailer": l["retailer"], "label": RETAILER_LABELS.get(l["retailer"], l["retailer"]),
                    "status": l["status"], "url": l["url"]}
            p = prods.get(l["retailer_ref"]) if l["retailer"] == "bestbuy" else None
            if p:
                price = float(p["sale_price"])
                item.update({
                    "sku": p["sku"], "price": price, "url": p["url"] or l["url"],
                    "in_stock": in_stock(p["orderable"], p["online_available"]),
                    "held_days": max(0, (now - p["price_since"]).days) if p["price_since"] else None,
                    "all_time_low": float(p["all_time_low"]) if p["all_time_low"] is not None else None,
                    "history_days": live_history_days({"history_days": p["history_days"],
                                                       "updated_at": p["stats_updated_at"]}, now),
                    "change_since_added": round(price - float(w["price_when_added"]), 2)
                                          if w["price_when_added"] is not None else None,
                })
                if item["in_stock"] and (best is None or price < best["price"]):
                    best = {"price": price, "retailer": item["label"]}
            ls.append(item)
        target = float(w["target_price"]) if w["target_price"] is not None else None
        la = last_alert.get(w["id"])
        out.append({
            "id": w["id"], "name": w["name"], "model_number": w["model_number"], "upc": w["upc"],
            "target_price": target,
            "price_when_added": float(w["price_when_added"]) if w["price_when_added"] is not None else None,
            "created_at": w["created_at"].isoformat(),
            "snoozed_until": w["snoozed_until"].isoformat() if w["snoozed_until"] and w["snoozed_until"] > now else None,
            "rules": {k: w[k] for k in ("alert_drop", "alert_below_target", "alert_all_time_low",
                                         "alert_back_in_stock", "alert_price_up")},
            "listings": ls, "best": best,
            "below_target": bool(best and target and best["price"] <= target),
            "last_alert_at": la["created_at"].isoformat() if la else None,
        })
    return out
