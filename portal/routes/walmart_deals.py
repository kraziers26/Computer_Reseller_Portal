"""
routes/walmart_deals.py

Flask blueprint for the Walmart Deal Blaster tab.
Mirrors the pattern of routes/deals.py (Best Buy) exactly.

Routes:
  GET  /walmart-deals              → renders the Walmart deals page
  GET  /api/walmart/deals          → returns active deals from wm_deals as JSON
  POST /api/walmart/deals/run      → on-demand scan with optional filters
  GET  /api/walmart/deals/status   → last run info for the scan bar
"""

import logging
from datetime import datetime, timedelta

from flask import Blueprint, jsonify, request, render_template
from flask_login import login_required
from psycopg.types.json import Json

from ..db import get_db
from ..services.wm_fetcher import fetch_walmart_deals, CATEGORIES

logger = logging.getLogger(__name__)

walmart_deals_bp = Blueprint('walmart_deals', __name__)


# ── Page ──────────────────────────────────────────────────────────────────────

@walmart_deals_bp.route('/walmart-deals')
@login_required
def walmart_deals_page():
    from flask import redirect, url_for
    return redirect(url_for('deals.deals_page') + '?retailer=walmart')


# ── Deals feed ────────────────────────────────────────────────────────────────

@walmart_deals_bp.route('/api/walmart/deals')
@login_required
def get_walmart_deals():
    """
    Returns active deals from wm_deals as JSON.
    Supports the same filter params as Best Buy:
      ?brands=Apple,Dell
      ?categories=gaming_laptops,macbooks
      ?min_price=0&max_price=2000
      ?min_score=7
      ?min_discount=10
    """
    conn = None
    try:
        conn = get_db()

        conditions = ["is_active = TRUE", "expires_at > NOW()"]
        params     = []

        brands = request.args.get('brands')
        if brands:
            brand_list = [b.strip() for b in brands.split(',') if b.strip()]
            if brand_list:
                placeholders = ','.join(['%s'] * len(brand_list))
                conditions.append(f"LOWER(brand) IN ({placeholders})")
                params.extend([b.lower() for b in brand_list])

        categories = request.args.get('categories')
        if categories:
            cat_list = [c.strip() for c in categories.split(',') if c.strip()]
            if cat_list:
                placeholders = ','.join(['%s'] * len(cat_list))
                conditions.append(f"category IN ({placeholders})")
                params.extend(cat_list)

        min_price = request.args.get('min_price')
        max_price = request.args.get('max_price')
        if min_price:
            conditions.append("sale_price >= %s")
            params.append(float(min_price))
        if max_price:
            conditions.append("sale_price <= %s")
            params.append(float(max_price))

        min_score = request.args.get('min_score')
        if min_score:
            conditions.append("score >= %s")
            params.append(int(min_score))

        min_discount = request.args.get('min_discount')
        if min_discount:
            conditions.append("discount_pct >= %s")
            params.append(float(min_discount))

        where = ' AND '.join(conditions)

        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    id, item_id, name, brand, category,
                    sale_price, reg_price, discount_pct, score,
                    url, in_stock, fetched_at
                FROM wm_deals
                WHERE {where}
                ORDER BY score DESC, fetched_at DESC
                LIMIT 100
            """, params)
            rows = cur.fetchall()

        deals = []
        for r in rows:
            deals.append({
                'id':           r['id'],
                'item_id':      r['item_id'],
                'name':         r['name'],
                'brand':        r['brand'] or '',
                'category':     r['category'] or '',
                'sale_price':   float(r['sale_price']),
                'reg_price':    float(r['reg_price']),
                'discount_pct': float(r['discount_pct']),
                'score':        r['score'],
                'url':          r['url'] or '',
                'in_stock':     r['in_stock'],
                'fetched_at':   r['fetched_at'].isoformat() if r['fetched_at'] else None,
            })

        return jsonify({'ok': True, 'deals': deals, 'count': len(deals)})

    except Exception as e:
        logger.error(f"[/api/walmart/deals] {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


# ── On-demand scan ────────────────────────────────────────────────────────────

@walmart_deals_bp.route('/api/walmart/deals/run', methods=['POST'])
@login_required
def run_walmart_scan():
    """
    Trigger an on-demand Walmart scan.
    Body (JSON, all optional):
    {
        "categories": ["gaming_laptops", "macbooks"],
        "min_discount": 10
    }
    """
    conn = None
    try:
        data         = request.get_json(silent=True) or {}
        categories   = data.get('categories') or None   # None = all cats
        min_discount = float(data.get('min_discount', 5))

        start = datetime.utcnow()
        result = fetch_walmart_deals(categories=categories, min_discount=min_discount)
        duration_ms = int((datetime.utcnow() - start).total_seconds() * 1000)

        if not result['ok']:
            return jsonify({'ok': False, 'error': result.get('error', 'Fetch failed')}), 500

        items = result['items']
        conn = get_db()

        # Upsert deals — update on item_id conflict
        new_count     = 0
        updated_count = 0
        expires_at    = datetime.utcnow() + timedelta(hours=24)

        with conn.cursor() as cur:
            # Mark all existing active as expired first (clean refresh approach)
            cur.execute("UPDATE wm_deals SET is_active = FALSE WHERE is_active = TRUE")

            for item in items:
                cur.execute("""
                    INSERT INTO wm_deals
                        (item_id, name, brand, category,
                         sale_price, reg_price, discount_pct, score,
                         url, in_stock, fetched_at, is_active, expires_at)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,%s)
                    ON CONFLICT (item_id) DO UPDATE SET
                        name         = EXCLUDED.name,
                        brand        = EXCLUDED.brand,
                        sale_price   = EXCLUDED.sale_price,
                        reg_price    = EXCLUDED.reg_price,
                        discount_pct = EXCLUDED.discount_pct,
                        score        = EXCLUDED.score,
                        url          = EXCLUDED.url,
                        in_stock     = EXCLUDED.in_stock,
                        fetched_at   = EXCLUDED.fetched_at,
                        is_active    = TRUE,
                        expires_at   = EXCLUDED.expires_at
                    RETURNING (xmax = 0) AS is_insert
                """, (
                    item['item_id'], item['name'], item['brand'],
                    item['category'], item['sale_price'], item['reg_price'],
                    item['discount_pct'], item['score'], item['url'],
                    item['in_stock'], item['fetched_at'], expires_at
                ))
                row = cur.fetchone()
                if row and row['is_insert']:
                    new_count += 1
                else:
                    updated_count += 1

            # Log this run
            cur.execute("""
                INSERT INTO wm_scan_runs (run_at, deals_found, new_deals, duration_ms, status, triggered_by)
                VALUES (%s, %s, %s, %s, 'ok', 'manual')
            """, (datetime.utcnow(), len(items), new_count, duration_ms))

        conn.commit()

        logger.info(
            f"[WM scan] {len(items)} deals — {new_count} new, "
            f"{updated_count} updated — {duration_ms}ms"
        )
        return jsonify({
            'ok':          True,
            'deals_found': len(items),
            'new_deals':   new_count,
            'duration_ms': duration_ms,
        })

    except Exception as e:
        logger.error(f"[/api/walmart/deals/run] {e}")
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return jsonify({'ok': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


# ── Status bar ────────────────────────────────────────────────────────────────

@walmart_deals_bp.route('/api/walmart/deals/status')
@login_required
def get_walmart_status():
    """Returns scan bar info: last run time, deal count."""
    conn = None
    try:
        conn = get_db()
        with conn.cursor() as cur:

            cur.execute("""
                SELECT run_at, deals_found, new_deals, duration_ms
                FROM wm_scan_runs
                WHERE status = 'ok'
                ORDER BY run_at DESC
                LIMIT 1
            """)
            last_run = cur.fetchone()

            cur.execute("""
                SELECT COUNT(*) as cnt
                FROM wm_deals
                WHERE is_active = TRUE AND expires_at > NOW()
            """)
            deal_count = cur.fetchone()['cnt']

        return jsonify({
            'ok':         True,
            'deal_count': deal_count,
            'last_run':   {
                'run_at':      last_run['run_at'].isoformat() if last_run else None,
                'deals_found': last_run['deals_found'] if last_run else 0,
                'new_deals':   last_run['new_deals'] if last_run else 0,
                'duration_ms': last_run['duration_ms'] if last_run else None,
            } if last_run else None,
        })

    except Exception as e:
        logger.error(f"[/api/walmart/deals/status] {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


# ── Categories list (for filter UI) ──────────────────────────────────────────

@walmart_deals_bp.route('/api/walmart/categories')
@login_required
def get_walmart_categories():
    """Returns the list of available Walmart categories for the filter UI."""
    return jsonify({
        'ok': True,
        'categories': [
            {'key': k, 'name': v['name']}
            for k, v in CATEGORIES.items()
        ]
    })
