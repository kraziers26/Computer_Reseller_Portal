"""
routes/price_drops.py

Deal Scanner — Price Drops, Watchlist, SKU detail and Telegram alert settings.

Pages:
  GET  /deals/drops                    Price Drops tab
  GET  /deals/watchlist                Watchlist tab
  GET  /deals/sku/<sku>                SKU detail (price history + score)
  GET  /deals/alerts                   Telegram alert settings
  GET  /deals/w/<token>/<action>       Snooze / Stop page opened from a Telegram button
  POST /deals/w/<token>/<action>       … confirm

API:
  GET  /api/drops?window=6&category=all     drops + KPIs
  GET  /api/drops/fake?category=all         "fake sale" list
  GET  /api/drops/status                    sync status strip
  POST /api/drops/sync                      run a sync now
  GET  /api/drops/sku/<sku>                 SKU detail data
  GET  /api/watchlist                       watched products
  POST /api/watchlist/match                 link / model / UPC → candidates
  POST /api/watchlist                       start watching {sku, target_price, other_url}
  PUT  /api/watchlist/<id>                  target, rules, snooze
  DELETE /api/watchlist/<id>                stop watching
  GET  /api/alerts/settings                 settings + connection info
  PUT  /api/alerts/settings                 save settings
  POST /api/alerts/test                     send a test message
  GET  /api/drops/bb-fields/<sku>           diagnostics: every field Best Buy returns for a SKU
"""

import logging
from flask import Blueprint, jsonify, request, render_template, abort, redirect, url_for
from flask_login import login_required, current_user

from ..db import get_db
from ..security import audit
from ..services import price_engine, watchlist, alerts, bestbuy

logger = logging.getLogger(__name__)

price_drops_bp = Blueprint('price_drops', __name__)

WINDOWS = {"1": 1, "6": 6, "24": 24, "168": 168}


def _conn():
    return get_db()


# ── Pages ─────────────────────────────────────────────────────────────────────

@price_drops_bp.route('/deals/drops')
@login_required
def drops_page():
    return render_template('deals/drops.html', view='drops')


@price_drops_bp.route('/deals/watchlist')
@login_required
def watchlist_page():
    return render_template('deals/watchlist.html', view='watchlist')


@price_drops_bp.route('/deals/sku/<sku>')
@login_required
def sku_page(sku):
    if not sku.isdigit():
        abort(404)
    return render_template('deals/sku.html', view='drops', sku=sku)


@price_drops_bp.route('/deals/alerts')
@login_required
def alerts_page():
    return render_template('deals/alerts.html', view='alerts', timezones=alerts.TIMEZONES)


@price_drops_bp.route('/deals/w/<token>/<action>', methods=['GET', 'POST'])
@login_required
def watch_action(token, action):
    if action not in ('snooze', 'stop'):
        abort(404)
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM watch_products WHERE action_token = %s", (token,))
            w = cur.fetchone()
        if not w:
            abort(404)
        done = False
        if request.method == 'POST':
            if action == 'snooze':
                watchlist.update_watch(conn, w['id'], {"snooze_hours": 24})
            else:
                watchlist.stop_watch(conn, w['id'])
            audit(f'watch_{action}', 'watch_product', w['id'], w['name'][:200])
            done = True
        return render_template('deals/watch_action.html', view='watchlist', w=w, action=action, done=done)
    finally:
        conn.close()


# ── Drops API ─────────────────────────────────────────────────────────────────

@price_drops_bp.route('/api/drops')
@login_required
def api_drops():
    window = WINDOWS.get(request.args.get('window', '6'), 6)
    category = request.args.get('category', 'all')
    conn = _conn()
    try:
        return jsonify(price_engine.list_drops(conn, window_hours=window, category=category))
    finally:
        conn.close()


@price_drops_bp.route('/api/drops/fake')
@login_required
def api_fake():
    conn = _conn()
    try:
        return jsonify({"items": price_engine.list_fake_sales(conn, request.args.get('category', 'all'))})
    finally:
        conn.close()


@price_drops_bp.route('/api/drops/status')
@login_required
def api_status():
    conn = _conn()
    try:
        return jsonify(price_engine.sync_status(conn))
    finally:
        conn.close()


@price_drops_bp.route('/api/drops/sync', methods=['POST'])
@login_required
def api_sync_now():
    res = price_engine.run_price_sync(kind="delta", scheduled=False)
    audit('price_sync_manual', 'price_sync', None, str(res)[:200])
    return jsonify(res), (200 if res.get("ok") else 502)


@price_drops_bp.route('/api/drops/sku/<sku>')
@login_required
def api_sku(sku):
    conn = _conn()
    try:
        d = price_engine.sku_detail(conn, sku)
        if not d:
            return jsonify({"error": "We haven't tracked this SKU yet."}), 404
        with conn.cursor() as cur:
            cur.execute("""SELECT w.id FROM watch_products w JOIN watch_listings l ON l.product_id = w.id
                           WHERE w.is_active AND l.retailer = 'bestbuy' AND l.retailer_ref = %s""", (sku,))
            row = cur.fetchone()
        d["watch_id"] = row["id"] if row else None
        return jsonify(d)
    finally:
        conn.close()


@price_drops_bp.route('/api/drops/bb-fields/<sku>')
@login_required
def api_bb_fields(sku):
    """Diagnostics: shows every field Best Buy returns for one SKU, so we can
    confirm which seller / marketplace fields exist before filtering on them."""
    if not sku.isdigit():
        abort(404)
    try:
        p = bestbuy.fetch_raw_product(sku)
    except bestbuy.BBApiError as e:
        return jsonify({"error": str(e)}), 502
    interesting = {k: v for k, v in p.items()
                   if any(w in k.lower() for w in ("seller", "market", "condition", "sold", "fulfil", "ship"))}
    return jsonify({"field_names": sorted(p.keys()), "seller_related": interesting})


# ── Watchlist API ─────────────────────────────────────────────────────────────

@price_drops_bp.route('/api/watchlist')
@login_required
def api_watchlist():
    conn = _conn()
    try:
        return jsonify({"items": watchlist.list_watches(conn)})
    finally:
        conn.close()


@price_drops_bp.route('/api/watchlist/match', methods=['POST'])
@login_required
def api_watch_match():
    text = (request.get_json(silent=True) or {}).get('input', '')
    try:
        return jsonify(watchlist.match(text))
    except watchlist.WatchError as e:
        return jsonify({"error": str(e)}), 400
    except bestbuy.BBApiError as e:
        return jsonify({"error": f"Best Buy API error: {e}"}), 502


@price_drops_bp.route('/api/watchlist', methods=['POST'])
@login_required
def api_watch_create():
    data = request.get_json(silent=True) or {}
    sku = str(data.get('sku') or '')
    if not sku.isdigit():
        return jsonify({"error": "Pick a Best Buy match first."}), 400
    conn = _conn()
    try:
        wid = watchlist.create_watch(conn, sku, data.get('target_price'), data.get('other_url'),
                                     data.get('rules'), getattr(current_user, 'id', None))
        audit('watch_create', 'watch_product', wid, sku)
        return jsonify({"ok": True, "id": wid})
    except watchlist.WatchError as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 400
    except bestbuy.BBApiError as e:
        conn.rollback()
        return jsonify({"error": f"Best Buy API error: {e}"}), 502
    finally:
        conn.close()


@price_drops_bp.route('/api/watchlist/<int:wid>', methods=['PUT'])
@login_required
def api_watch_update(wid):
    conn = _conn()
    try:
        watchlist.update_watch(conn, wid, request.get_json(silent=True) or {})
        return jsonify({"ok": True})
    except watchlist.WatchError as e:
        return jsonify({"error": str(e)}), 400
    finally:
        conn.close()


@price_drops_bp.route('/api/watchlist/<int:wid>', methods=['DELETE'])
@login_required
def api_watch_delete(wid):
    conn = _conn()
    try:
        watchlist.stop_watch(conn, wid)
        audit('watch_stop', 'watch_product', wid, None)
        return jsonify({"ok": True})
    finally:
        conn.close()


# ── Alert settings API ────────────────────────────────────────────────────────

@price_drops_bp.route('/api/alerts/settings')
@login_required
def api_alert_settings():
    conn = _conn()
    try:
        return jsonify({"settings": alerts.get_settings(conn), "connection": alerts.connection_info(),
                        "timezones": alerts.TIMEZONES})
    finally:
        conn.close()


@price_drops_bp.route('/api/alerts/settings', methods=['PUT'])
@login_required
def api_alert_settings_save():
    conn = _conn()
    try:
        s = alerts.save_settings(conn, request.get_json(silent=True) or {})
        audit('alert_settings_save', 'alert_settings', 1, None)
        return jsonify({"ok": True, "settings": s})
    finally:
        conn.close()


@price_drops_bp.route('/api/alerts/test', methods=['POST'])
@login_required
def api_alert_test():
    conn = _conn()
    try:
        return jsonify({"results": alerts.send_test(conn), "connection": alerts.connection_info()})
    finally:
        conn.close()
