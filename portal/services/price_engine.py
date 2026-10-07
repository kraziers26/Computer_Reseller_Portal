"""
services/price_engine.py

Price-drop engine for the Deal Scanner (Best Buy).

Every 15 minutes (services/scheduler.py → run_price_sync):
  1. Fetch only SKUs whose price changed recently (delta), plus every watched SKU.
     Every few hours, a full category sweep also catches stock changes.
  2. Compare each product with what we stored last time (bb_products).
  3. On any change: write bb_price_history, classify a bb_price_events row
     (drop / rise / sold_out / back_in_stock), refresh bb_sku_stats.
  4. Hand the new events to services/alerts.py for Telegram.

History starts the day this ships. On first sight of a SKU we also store
Best Buy's priceUpdateDate as price_since, so "this price has held for
41 days" works from day one.
"""

import logging
import statistics
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from psycopg.types.json import Json

from . import bestbuy
from ..db import get_db

logger = logging.getLogger(__name__)

UTC = timezone.utc
# Best Buy returns dates without a timezone. We assume US Central
# (Best Buy HQ). Only affects "held since" display by a few hours.
BB_TZ = ZoneInfo("America/Chicago")

DELTA_WINDOW_HOURS    = 8      # generous overlap — covers any US timezone interpretation
FULL_SWEEP_EVERY_MIN  = 180    # full category sweep every 3 hours
SYNC_EVERY_MIN        = 15
ATL_MIN_HISTORY_DAYS  = 14     # don't call anything "all-time low" before 2 weeks of data
MEDIAN_MIN_DAYS       = 7      # use 30-day median as reference once we have a week
FAKE_SALE_CLAIM_PCT   = 10     # Best Buy claims ≥10% off ...
FAKE_SALE_HELD_DAYS   = 30     # ... but the price hasn't moved in 30+ days
PRICE_EPS             = 0.009
SYNC_LOCK_KEY         = 731_004   # pg advisory lock id for "price sync running"


# ── Small helpers ─────────────────────────────────────────────────────────────

def now_utc():
    return datetime.now(UTC)


def parse_bb_date(s):
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(str(s).replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=BB_TZ)
    return dt.astimezone(UTC)


def _f(v):
    try:
        return round(float(v), 2) if v is not None and v != "" else None
    except Exception:
        return None


def _i(v):
    try:
        return int(v) if v is not None and v != "" else None
    except Exception:
        return None


def normalize(p: dict) -> dict:
    online = p.get("onlineAvailability")
    return {
        "sku":           str(p.get("sku") or "").strip(),
        "name":          (p.get("name") or "")[:255],
        "brand":         (p.get("manufacturer") or "")[:100] or None,
        "model_number":  (p.get("modelNumber") or "")[:100] or None,
        "upc":           (str(p.get("upc") or ""))[:32] or None,
        "url":           p.get("url") or None,
        "image_url":     p.get("image") or None,
        "condition":     (p.get("condition") or "")[:30] or None,
        "sale_price":    _f(p.get("salePrice")),
        "regular_price": _f(p.get("regularPrice")),
        "orderable":     (p.get("orderable") or "")[:30] or None,
        "online":        None if online is None else bool(online),
        "qty_limit":     _i(p.get("quantityLimit")),
        "bb_update":     parse_bb_date(p.get("priceUpdateDate")),
    }


def in_stock(orderable, online) -> bool:
    if online is False:
        return False
    return (orderable or "Available") == "Available"


# ── Change detection ──────────────────────────────────────────────────────────

def process_products(conn, products: list, category: str = None, now=None) -> list:
    """Compare fetched products with stored state; record changes.
    Returns the list of new events (dicts). Does not commit."""
    now = now or now_utc()
    events, changed = [], set()

    with conn.cursor() as cur:
        for raw in products:
            n = normalize(raw)
            if not n["sku"] or n["sale_price"] is None:
                continue
            sku = n["sku"]
            cur.execute("SELECT * FROM bb_products WHERE sku = %s FOR UPDATE", (sku,))
            old = cur.fetchone()

            if not old:
                price_since = n["bb_update"] if n["bb_update"] and n["bb_update"] < now else now
                cur.execute("""
                    INSERT INTO bb_products (sku, name, brand, category, model_number, upc, url, image_url,
                        condition, sale_price, regular_price, orderable, online_available, qty_limit,
                        bb_price_update_date, price_since, first_seen_at, last_seen_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (sku, n["name"], n["brand"], category, n["model_number"], n["upc"], n["url"],
                      n["image_url"], n["condition"], n["sale_price"], n["regular_price"], n["orderable"],
                      n["online"], n["qty_limit"], n["bb_update"], price_since, now, now))
                _insert_history(cur, sku, n, now)
                events.append(_insert_event(cur, sku, "first_seen", None, n["sale_price"], None, now))
                changed.add(sku)
                continue

            old_price = float(old["sale_price"])
            price_changed = abs(old_price - n["sale_price"]) > PRICE_EPS
            was_in = in_stock(old["orderable"], old["online_available"])
            now_in = in_stock(n["orderable"], n["online"])
            stock_changed = was_in != now_in

            if price_changed or stock_changed:
                _insert_history(cur, sku, n, now)
                changed.add(sku)

            if price_changed:
                etype = "drop" if n["sale_price"] < old_price else "rise"
                events.append(_insert_event(cur, sku, etype, old_price, n["sale_price"],
                                            old["price_since"], now))
            if stock_changed:
                etype = "back_in_stock" if now_in else "sold_out"
                events.append(_insert_event(cur, sku, etype, old_price, n["sale_price"],
                                            old["price_since"], now))

            new_category = old["category"]
            if category and (not old["category"] or old["category"] == "watch"):
                new_category = category

            cur.execute("""
                UPDATE bb_products SET
                    name = %s, brand = %s, category = %s,
                    model_number = COALESCE(%s, model_number), upc = COALESCE(%s, upc),
                    url = COALESCE(%s, url), image_url = COALESCE(%s, image_url),
                    condition = COALESCE(%s, condition),
                    sale_price = %s, regular_price = %s, orderable = %s, online_available = %s,
                    qty_limit = %s, bb_price_update_date = %s,
                    price_since = CASE WHEN %s THEN %s ELSE price_since END,
                    last_seen_at = %s
                WHERE sku = %s
            """, (n["name"], n["brand"], new_category, n["model_number"], n["upc"], n["url"],
                  n["image_url"], n["condition"], n["sale_price"], n["regular_price"], n["orderable"],
                  n["online"], n["qty_limit"], n["bb_update"], price_changed, now, now, sku))

        for sku in changed:
            recompute_stats(cur, sku, now)

        # Attach flags (depth, all-time-low status…) now that stats are fresh
        for ev in events:
            if ev["event_type"] in ("drop", "back_in_stock"):
                ev["flags"] = compute_flags(cur, ev, now)
                cur.execute("UPDATE bb_price_events SET flags = %s WHERE id = %s",
                            (Json(ev["flags"]), ev["id"]))
    return events


def _insert_history(cur, sku, n, now):
    cur.execute("""
        INSERT INTO bb_price_history (sku, captured_at, sale_price, regular_price, orderable,
                                      online_available, qty_limit, bb_price_update_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """, (sku, now, n["sale_price"], n["regular_price"], n["orderable"], n["online"],
          n["qty_limit"], n["bb_update"]))


def _insert_event(cur, sku, etype, old_price, new_price, old_since, now):
    cur.execute("""
        INSERT INTO bb_price_events (sku, event_type, old_price, new_price, old_price_since, detected_at)
        VALUES (%s,%s,%s,%s,%s,%s) RETURNING id
    """, (sku, etype, old_price, new_price, old_since, now))
    return {"id": cur.fetchone()["id"], "sku": sku, "event_type": etype, "old_price": old_price,
            "new_price": new_price, "old_price_since": old_since, "detected_at": now, "flags": {}}


# ── Stats ─────────────────────────────────────────────────────────────────────

def price_segments(cur, sku, now):
    """[(start, end, price)] for the SKU, oldest first. The first segment is
    extended back to Best Buy's priceUpdateDate when that is earlier than our
    first observation (we know the price held since then)."""
    cur.execute("""
        SELECT captured_at, sale_price, bb_price_update_date
        FROM bb_price_history WHERE sku = %s ORDER BY captured_at, id
    """, (sku,))
    rows = cur.fetchall()
    if not rows:
        return []
    points = []
    for r in rows:
        price = float(r["sale_price"])
        if points and abs(points[-1][1] - price) <= PRICE_EPS:
            continue                      # stock-only change, same price
        points.append((r["captured_at"], price))
    first_bb = rows[0]["bb_price_update_date"]
    if first_bb and first_bb < points[0][0]:
        points[0] = (first_bb, points[0][1])
    segs = []
    for i, (start, price) in enumerate(points):
        end = points[i + 1][0] if i + 1 < len(points) else now
        segs.append((start, end, price))
    return segs


def price_at(segs, t):
    p = None
    for start, end, price in segs:
        if start <= t:
            p = price
    return p


def recompute_stats(cur, sku, now):
    segs = price_segments(cur, sku, now)
    if not segs:
        return
    start = segs[0][0]
    history_days = max(0, (now - start).days)
    prices = [s[2] for s in segs]
    atl = min(prices)
    atl_at = next(s[0] for s in segs if s[2] == atl)
    prev_low = min(prices[:-1]) if len(prices) > 1 else None

    since_30 = now - timedelta(days=30)
    in_30 = [s[2] for s in segs if s[1] > since_30]
    low_30 = min(in_30) if in_30 else None

    # Median of the daily closing price over the last 30 days we have data for
    daily = []
    day = max(start, since_30).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
    while day < now:
        p = price_at(segs, day)
        if p is not None:
            daily.append(p)
        day += timedelta(days=1)
    daily.append(segs[-1][2])
    median_30 = round(statistics.median(daily), 2) if daily else None

    cur.execute("""
        INSERT INTO bb_sku_stats (sku, all_time_low, all_time_low_at, prev_low, low_30d,
                                  median_30d, history_days, updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (sku) DO UPDATE SET
            all_time_low = EXCLUDED.all_time_low, all_time_low_at = EXCLUDED.all_time_low_at,
            prev_low = EXCLUDED.prev_low, low_30d = EXCLUDED.low_30d,
            median_30d = EXCLUDED.median_30d, history_days = EXCLUDED.history_days,
            updated_at = EXCLUDED.updated_at
    """, (sku, atl, atl_at, prev_low, low_30, median_30, history_days, now))


def compute_flags(cur, ev, now) -> dict:
    cur.execute("SELECT * FROM bb_sku_stats WHERE sku = %s", (ev["sku"],))
    s = cur.fetchone() or {}
    new = float(ev["new_price"])
    hist_days = int(s.get("history_days") or 0)
    median = float(s["median_30d"]) if s.get("median_30d") is not None else None

    # The median already includes today's new price; use it as the reference
    # only when there's enough history for one day not to dominate.
    if hist_days >= MEDIAN_MIN_DAYS and median and median > new:
        ref, ref_kind = median, "median_30d"
    elif ev.get("old_price"):
        ref, ref_kind = float(ev["old_price"]), "previous"
    else:
        ref, ref_kind = None, None
    depth = round((ref - new) / ref * 100, 1) if ref else 0.0

    prev_low = float(s["prev_low"]) if s.get("prev_low") is not None else None
    if hist_days < ATL_MIN_HISTORY_DAYS or prev_low is None:
        low_status, above_by = "unknown", None
    elif new < prev_low - PRICE_EPS:
        low_status, above_by = "new_low", None
    elif abs(new - prev_low) <= 0.5:
        low_status, above_by = "ties_low", None
    else:
        low_status, above_by = "above_low", round(new - prev_low, 2)

    held_days = None
    if ev.get("old_price_since"):
        held_days = max(0, (ev["detected_at"] - ev["old_price_since"]).days)

    return {"depth_pct": depth, "ref": ref, "ref_kind": ref_kind, "low_status": low_status,
            "above_low_by": above_by, "prev_low": prev_low, "held_days": held_days,
            "history_days": hist_days}


# ── Scoring (0–100) ───────────────────────────────────────────────────────────

def score_event(ev: dict, product: dict, now=None) -> dict:
    """Freshness 35 · real depth 30 · history position 20 · buyability 15."""
    now = now or now_utc()
    flags = ev.get("flags") or {}
    mins = (now - ev["detected_at"]).total_seconds() / 60
    fresh = (35 if mins <= 30 else 30 if mins <= 120 else 22 if mins <= 360
             else 12 if mins <= 1440 else 5 if mins <= 4320 else 0)

    depth_pct = max(0.0, float(flags.get("depth_pct") or 0))
    depth = round(30 * min(depth_pct, 15) / 15)

    ls = flags.get("low_status")
    if ls == "new_low":
        hist = 20
    elif ls == "ties_low":
        hist = 15
    elif ls == "above_low" and flags.get("prev_low") and \
            (flags.get("above_low_by") or 0) <= 0.03 * float(flags["prev_low"]):
        hist = 8
    elif ls == "unknown" and (flags.get("held_days") or 0) >= 30:
        hist = 10     # early days: old price had held a month, so this drop is meaningful
    else:
        hist = 0

    buy = 0
    if in_stock(product.get("orderable"), product.get("online_available")):
        buy = 9
        q = product.get("qty_limit")
        buy += 6 if (q is None or q >= 5) else 3 if q >= 2 else 0

    return {"total": fresh + depth + hist + buy, "freshness": fresh, "depth": depth,
            "history": hist, "buyability": buy}


def is_fake_sale(product: dict, stats: dict, now=None) -> bool:
    """Best Buy claims a big saving, but the price hasn't actually moved."""
    now = now or now_utc()
    sale = float(product.get("sale_price") or 0)
    reg = float(product.get("regular_price") or 0)
    if not sale or not reg or reg <= sale:
        return False
    claim = (reg - sale) / reg * 100
    if claim < FAKE_SALE_CLAIM_PCT:
        return False
    since = product.get("price_since")
    if since and (now - since).days >= FAKE_SALE_HELD_DAYS:
        return True
    median = (stats or {}).get("median_30d")
    if median and int((stats or {}).get("history_days") or 0) >= 21 and sale >= float(median) * 0.99:
        return True
    return False


# ── Sync job ──────────────────────────────────────────────────────────────────

def claim_job(conn, name: str, min_gap_minutes: float) -> bool:
    """Atomically claim a job slot so only one Gunicorn worker runs it.
    If migration 004 hasn't been run yet (no job_claims table), allow the
    job so existing schedules keep working exactly as before."""
    import psycopg
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO job_claims (job_name) VALUES (%s) ON CONFLICT DO NOTHING", (name,))
            cur.execute("""
                UPDATE job_claims SET last_claimed_at = NOW()
                WHERE job_name = %s AND last_claimed_at < NOW() - make_interval(secs => %s)
                RETURNING job_name
            """, (name, min_gap_minutes * 60))
            ok = cur.fetchone() is not None
        conn.commit()
        return ok
    except psycopg.errors.UndefinedTable:
        conn.rollback()
        logger.warning("[Scheduler] job_claims table missing — run migration 004")
        return True


def watched_skus(conn) -> list:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT l.retailer_ref AS sku
            FROM watch_listings l JOIN watch_products w ON w.id = l.product_id
            WHERE w.is_active AND l.retailer = 'bestbuy' AND l.status = 'tracked'
        """)
        return [r["sku"] for r in cur.fetchall() if r["sku"]]


def run_price_sync(kind: str = "delta", scheduled: bool = True) -> dict:
    """Entry point for the scheduler (and the "Sync now" button).
    kind: 'delta' (changed SKUs + watched) or 'full' (whole categories)."""
    from . import alerts   # local import avoids a cycle

    conn = get_db()
    run_id = None
    counter = bestbuy.CallCounter()
    try:
        if scheduled and not claim_job(conn, "price_sync", SYNC_EVERY_MIN * 0.66):
            return {"ok": True, "skipped": True}

        # One sync at a time across both workers and the "Sync now" button.
        # Session-level lock: released automatically if the process dies.
        with conn.cursor() as cur:
            cur.execute("SELECT pg_try_advisory_lock(%s) AS ok", (SYNC_LOCK_KEY,))
            got = cur.fetchone()["ok"]
        conn.commit()
        if not got:
            return {"ok": True, "skipped": True, "reason": "a sync is already running"}

        # Full sweep every few hours (or the very first time)
        if kind == "delta":
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) AS n FROM bb_products")
                empty = cur.fetchone()["n"] == 0
            if empty or claim_job(conn, "price_full_sweep", FULL_SWEEP_EVERY_MIN):
                kind = "full"

        with conn.cursor() as cur:
            cur.execute("INSERT INTO price_sync_runs (kind) VALUES (%s) RETURNING id",
                        ("manual" if not scheduled else kind,))
            run_id = cur.fetchone()["id"]
        conn.commit()

        now = now_utc()
        events, checked = [], 0

        since = now - timedelta(hours=DELTA_WINDOW_HOURS)
        for cat in bestbuy.CATEGORIES:
            if kind == "full":
                prods = bestbuy.fetch_category_all(cat, counter)
            else:
                prods = bestbuy.fetch_category_changed_since(cat, since.astimezone(BB_TZ), counter)
            checked += len(prods)
            events += process_products(conn, prods, category=cat["slug"], now=now)
            conn.commit()

        skus = watched_skus(conn)
        if skus:
            prods = bestbuy.fetch_skus(skus, counter)
            checked += len(prods)
            events += process_products(conn, prods, category="watch", now=now)
            conn.commit()

        real_events = [e for e in events if e["event_type"] != "first_seen"]
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE price_sync_runs SET finished_at = NOW(), status = 'ok',
                    skus_checked = %s, changes = %s, api_calls = %s
                WHERE id = %s
            """, (checked, len(real_events), counter.calls, run_id))
        conn.commit()
        logger.info(f"[PriceSync] {kind}: {checked} SKUs, {len(real_events)} changes, {counter.calls} calls")

        try:
            alerts.after_sync(conn, real_events, run_ok=True)
        except Exception as e:
            conn.rollback()
            logger.error(f"[PriceSync] alerts failed: {e}")

        return {"ok": True, "kind": kind, "skus_checked": checked,
                "changes": len(real_events), "api_calls": counter.calls}

    except Exception as e:
        conn.rollback()
        logger.error(f"[PriceSync] FAILED: {e}")
        try:
            with conn.cursor() as cur:
                if run_id:
                    cur.execute("""UPDATE price_sync_runs SET finished_at = NOW(), status = 'error',
                                   error_message = %s, api_calls = %s WHERE id = %s""",
                                (str(e)[:500], counter.calls, run_id))
                else:
                    cur.execute("""INSERT INTO price_sync_runs (kind, finished_at, status, error_message)
                                   VALUES (%s, NOW(), 'error', %s)""", (kind, str(e)[:500]))
            conn.commit()
            alerts.after_sync(conn, [], run_ok=False)
        except Exception as e2:
            logger.error(f"[PriceSync] could not record failure: {e2}")
        return {"ok": False, "error": str(e)}
    finally:
        conn.close()


# ── Read side (used by routes) ────────────────────────────────────────────────

def _history_map(cur, skus, days=90):
    if not skus:
        return {}
    cur.execute("""
        SELECT sku, captured_at, sale_price FROM bb_price_history
        WHERE sku = ANY(%s) AND captured_at > NOW() - make_interval(days => %s)
        ORDER BY sku, captured_at
    """, (list(skus), days))
    out = {}
    for r in cur.fetchall():
        out.setdefault(r["sku"], []).append([r["captured_at"].isoformat(), float(r["sale_price"])])
    return out


def _product_view(p, s, now):
    return {
        "sku": p["sku"], "name": p["name"], "brand": p["brand"], "category": p["category"],
        "url": p["url"], "image_url": p["image_url"], "model_number": p["model_number"],
        "price": float(p["sale_price"]),
        "regular_price": float(p["regular_price"]) if p["regular_price"] is not None else None,
        "in_stock": in_stock(p["orderable"], p["online_available"]),
        "orderable": p["orderable"], "qty_limit": p["qty_limit"],
        "price_since": p["price_since"].isoformat() if p["price_since"] else None,
        "held_days": max(0, (now - p["price_since"]).days) if p["price_since"] else None,
        "all_time_low": float(s["all_time_low"]) if s and s.get("all_time_low") is not None else None,
        "median_30d": float(s["median_30d"]) if s and s.get("median_30d") is not None else None,
        "history_days": live_history_days(s, now),
    }


def live_history_days(s, now):
    """bb_sku_stats.history_days is stored when the SKU last changed; add the
    time since then so stable SKUs don't look newer than they are."""
    if not s or s.get("history_days") is None:
        return 0
    upd = s.get("updated_at")
    extra = (now - upd).days if upd else 0
    return int(s["history_days"]) + max(0, extra)


def list_drops(conn, window_hours=6, category=None, now=None) -> dict:
    now = now or now_utc()
    with conn.cursor() as cur:
        params = [now - timedelta(hours=window_hours)]
        cat_sql = ""
        if category and category != "all":
            cat_sql = "AND p.category = %s"
            params.append(category)
        cur.execute(f"""
            SELECT DISTINCT ON (e.sku) e.*, row_to_json(p.*) AS p, row_to_json(s.*) AS s
            FROM bb_price_events e
            JOIN bb_products p ON p.sku = e.sku
            LEFT JOIN bb_sku_stats s ON s.sku = e.sku
            WHERE e.event_type IN ('drop','back_in_stock') AND e.detected_at > %s {cat_sql}
            ORDER BY e.sku, e.detected_at DESC
        """, params)
        rows = cur.fetchall()
        hist = _history_map(cur, [r["sku"] for r in rows])

    items = []
    for r in rows:
        p = _jsonrow(r["p"])
        s = _jsonrow(r["s"]) if r["s"] else {}
        ev = {"id": r["id"], "sku": r["sku"], "event_type": r["event_type"],
              "old_price": float(r["old_price"]) if r["old_price"] is not None else None,
              "new_price": float(r["new_price"]), "detected_at": r["detected_at"],
              "old_price_since": r["old_price_since"], "flags": r["flags"] or {}}
        view = _product_view(p, s, now)
        ended = view["price"] > ev["new_price"] + PRICE_EPS or not view["in_stock"]
        sc = score_event(ev, p, now)
        items.append({
            **view,
            "event_id": ev["id"], "event_type": ev["event_type"],
            "detected_at": ev["detected_at"].isoformat(),
            "minutes_ago": max(0, int((now - ev["detected_at"]).total_seconds() // 60)),
            "old_price": ev["old_price"], "new_price": ev["new_price"],
            "flags": ev["flags"], "score": sc, "ended": ended,
            "history": hist.get(r["sku"], []),
        })
    items.sort(key=lambda x: (x["ended"], -x["score"]["total"]))
    return {"items": items, "kpis": kpis(conn, category, now)}


def _jsonrow(d):
    """row_to_json gives ISO strings for timestamps; turn them back into datetimes."""
    out = dict(d)
    for k in ("price_since", "first_seen_at", "last_seen_at", "bb_price_update_date",
              "all_time_low_at", "updated_at"):
        if out.get(k):
            out[k] = datetime.fromisoformat(out[k])
            if out[k].tzinfo is None:
                out[k] = out[k].replace(tzinfo=UTC)
    return out


def kpis(conn, category=None, now=None) -> dict:
    now = now or now_utc()
    day_ago = now - timedelta(hours=24)
    cat_sql, params = "", []
    if category and category != "all":
        cat_sql, params = "AND p.category = %s", [category]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT
              COUNT(*) FILTER (WHERE e.event_type = 'drop' AND e.flags->>'low_status' = 'new_low') AS new_lows,
              COUNT(*) FILTER (WHERE e.event_type = 'drop' AND (e.flags->>'depth_pct')::numeric >= 10) AS real_drops,
              COUNT(*) FILTER (WHERE e.event_type IN ('rise','sold_out') AND EXISTS (
                  SELECT 1 FROM bb_price_events d WHERE d.sku = e.sku AND d.event_type = 'drop'
                    AND d.detected_at BETWEEN e.detected_at - INTERVAL '7 days' AND e.detected_at)) AS died
            FROM bb_price_events e JOIN bb_products p ON p.sku = e.sku
            WHERE e.detected_at > %s {cat_sql}
        """, [day_ago] + params)
        k = cur.fetchone()
    fakes = list_fake_sales(conn, category, now, limit=500)
    return {"new_lows": k["new_lows"], "real_drops": k["real_drops"], "died": k["died"],
            "fake_sales": len(fakes)}


def list_fake_sales(conn, category=None, now=None, limit=100) -> list:
    now = now or now_utc()
    cat_sql, params = "", []
    if category and category != "all":
        cat_sql, params = "AND p.category = %s", [category]
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT row_to_json(p.*) AS p, row_to_json(s.*) AS s
            FROM bb_products p LEFT JOIN bb_sku_stats s ON s.sku = p.sku
            WHERE p.regular_price > p.sale_price * 1.1 {cat_sql}
              AND p.last_seen_at > NOW() - INTERVAL '2 days'
        """, params)
        rows = cur.fetchall()
    out = []
    for r in rows:
        p = _jsonrow(r["p"])
        s = _jsonrow(r["s"]) if r["s"] else {}
        if in_stock(p["orderable"], p["online_available"]) and is_fake_sale(p, s, now):
            v = _product_view(p, s, now)
            v["claim_pct"] = round((v["regular_price"] - v["price"]) / v["regular_price"] * 100)
            out.append(v)
    out.sort(key=lambda v: -v["claim_pct"])
    return out[:limit]


def sku_detail(conn, sku, now=None) -> dict:
    now = now or now_utc()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM bb_products WHERE sku = %s", (sku,))
        p = cur.fetchone()
        if not p:
            return None
        cur.execute("SELECT * FROM bb_sku_stats WHERE sku = %s", (sku,))
        s = cur.fetchone() or {}
        segs = price_segments(cur, sku, now)
        cur.execute("""SELECT * FROM bb_price_events WHERE sku = %s AND event_type <> 'first_seen'
                       ORDER BY detected_at DESC LIMIT 20""", (sku,))
        evs = cur.fetchall()

    view = _product_view(p, s, now)
    latest_drop = next((e for e in evs if e["event_type"] in ("drop", "back_in_stock")), None)
    score = None
    if latest_drop:
        ev = dict(latest_drop)
        ev["old_price"] = float(ev["old_price"]) if ev["old_price"] is not None else None
        ev["new_price"] = float(ev["new_price"])
        score = {"breakdown": score_event(ev, p, now), "flags": ev["flags"] or {},
                 "detected_at": ev["detected_at"].isoformat(),
                 "ended": view["price"] > ev["new_price"] + PRICE_EPS or not view["in_stock"]}

    claim = None
    if view["regular_price"] and view["regular_price"] > view["price"]:
        claim = {"save": round(view["regular_price"] - view["price"], 2),
                 "pct": round((view["regular_price"] - view["price"]) / view["regular_price"] * 100)}
    real = None
    ref = view["median_30d"] if view["history_days"] >= MEDIAN_MIN_DAYS else None
    if ref and ref > view["price"]:
        real = {"save": round(ref - view["price"], 2),
                "pct": round((ref - view["price"]) / ref * 100), "ref": ref}

    return {
        **view,
        "fake_sale": is_fake_sale(p, s, now),
        "claim": claim, "real": real, "score": score,
        "segments": [[a.isoformat(), b.isoformat(), price] for a, b, price in segs
                     if b > now - timedelta(days=90)],
        "events": [{"type": e["event_type"],
                    "old_price": float(e["old_price"]) if e["old_price"] is not None else None,
                    "new_price": float(e["new_price"]) if e["new_price"] is not None else None,
                    "at": e["detected_at"].isoformat(), "flags": e["flags"] or {}} for e in evs],
    }


def sync_status(conn, now=None) -> dict:
    now = now or now_utc()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM price_sync_runs ORDER BY started_at DESC LIMIT 1")
        last = cur.fetchone()
        cur.execute("""SELECT COALESCE(SUM(api_calls),0) AS calls FROM price_sync_runs
                       WHERE started_at > date_trunc('day', NOW())""")
        calls = cur.fetchone()["calls"]
        cur.execute("SELECT COUNT(*) AS n FROM bb_products")
        tracked = cur.fetchone()["n"]
        cur.execute("SELECT MIN(first_seen_at) AS t FROM bb_products")
        started = cur.fetchone()["t"]
    out = {"api_calls_today": int(calls), "skus_tracked": tracked,
           "history_started": started.isoformat() if started else None,
           "every_minutes": SYNC_EVERY_MIN}
    if last:
        out["last"] = {"status": last["status"], "kind": last["kind"],
                       "started_at": last["started_at"].isoformat(),
                       "changes": last["changes"], "skus_checked": last["skus_checked"],
                       "error": last["error_message"]}
        out["next_at"] = (last["started_at"] + timedelta(minutes=SYNC_EVERY_MIN)).isoformat()
    return out
