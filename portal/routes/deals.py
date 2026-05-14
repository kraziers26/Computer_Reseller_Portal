"""
routes/deals.py

Flask blueprint for the Deal Blaster — Deal Scan / Suggestions department.

Routes:
  GET  /deals                     → renders the Deal Blaster page
  GET  /api/deals                 → returns active deals from bb_deals as JSON
  POST /api/deals/run             → on-demand scan with filters from request body
  POST /api/deals/run/<sched_id>  → run a specific saved schedule on-demand
  GET  /api/deals/schedules       → list all schedules with job status
  PUT  /api/deals/schedules/<id>  → pause / resume a schedule
  GET  /api/deals/status          → last run info for the scan bar
"""

import logging
from flask import Blueprint, jsonify, request, render_template
from flask_login import login_required

from ..db import get_db
from ..services.scheduler import (
    run_manual_scan,
    pause_job,
    resume_job,
    get_job_status,
)

logger = logging.getLogger(__name__)

deals_bp = Blueprint('deals', __name__)


# ── Page ──────────────────────────────────────────────────────────────────────

@deals_bp.route('/deals')
@login_required
def deals_page():
    """Renders the Deal Blaster portal page."""
    return render_template('deals/index.html')


# ── Deals feed ────────────────────────────────────────────────────────────────

@deals_bp.route('/api/deals')
@login_required
def get_deals():
    """
    Returns active deals from bb_deals as JSON.
    Supports query params for filtering:
      ?brands=Apple,Dell
      ?categories=macbook,gaming
      ?min_price=0&max_price=2000
      ?cpu=Apple+M,Intel
      ?ram=16GB,32GB
      ?min_score=7
    """
    conn = None
    try:
        conn = get_db()

        # Build WHERE clauses from query params
        conditions = ["is_active = TRUE", "expires_at > NOW()"]
        params     = []

        brands = request.args.get('brands')
        if brands:
            brand_list = [b.strip() for b in brands.split(',') if b.strip()]
            if brand_list:
                placeholders = ','.join(['%s'] * len(brand_list))
                conditions.append(f"brand IN ({placeholders})")
                params.extend(brand_list)

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

        # CPU / RAM are text filters — use ILIKE
        cpu = request.args.get('cpu')
        if cpu:
            cpu_list = [c.strip() for c in cpu.split(',') if c.strip()]
            if cpu_list:
                cpu_clauses = ' OR '.join(['cpu ILIKE %s'] * len(cpu_list))
                conditions.append(f"({cpu_clauses})")
                params.extend([f'%{c}%' for c in cpu_list])

        ram = request.args.get('ram')
        if ram:
            ram_list = [r.strip() for r in ram.split(',') if r.strip()]
            if ram_list:
                placeholders = ','.join(['%s'] * len(ram_list))
                conditions.append(f"memory IN ({placeholders})")
                params.extend(ram_list)

        where = ' AND '.join(conditions)

        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    id, sku, name, brand, category,
                    sale_price, regular_price, discount_pct, score,
                    cpu, memory, url, fetched_at
                FROM bb_deals
                WHERE {where}
                ORDER BY score DESC, fetched_at DESC
                LIMIT 100
            """, params)
            rows = cur.fetchall()

        deals = []
        for r in rows:
            deals.append({
                'id':           r['id'],
                'sku':          r['sku'],
                'name':         r['name'],
                'brand':        r['brand'] or '',
                'category':     r['category'] or '',
                'sale_price':   float(r['sale_price']),
                'regular_price':float(r['regular_price']),
                'discount_pct': r['discount_pct'],
                'score':        r['score'],
                'cpu':          r['cpu'] or '',
                'memory':       r['memory'] or '',
                'url':          r['url'] or '',
                'fetched_at':   r['fetched_at'].isoformat() if r['fetched_at'] else None,
            })

        return jsonify({'ok': True, 'deals': deals, 'count': len(deals)})

    except Exception as e:
        logger.error(f"[/api/deals] {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


# ── On-demand scan ────────────────────────────────────────────────────────────

@deals_bp.route('/api/deals/run', methods=['POST'])
@login_required
def run_scan():
    """
    Trigger an on-demand scan with filters from the Deal Blaster UI.
    Body (JSON, all optional):
    {
        "brands":     ["Apple", "Dell"],
        "categories": ["macbook", "gaming"],
        "price_min":  0,
        "price_max":  2000,
        "cpu":        ["Apple M", "Intel"],
        "ram":        ["16GB"],
        "min_score":  7
    }
    """
    try:
        filters = request.get_json(silent=True) or {}
        result  = run_manual_scan(filters=filters)
        return jsonify(result), 200 if result['ok'] else 500
    except Exception as e:
        logger.error(f"[/api/deals/run] {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500


@deals_bp.route('/api/deals/run/<int:sched_id>', methods=['POST'])
@login_required
def run_schedule_now(sched_id):
    """
    Run a specific saved schedule on-demand (per-schedule Run Now button).
    Loads the schedule's saved filters and runs with them.
    """
    try:
        result = run_manual_scan(schedule_id=sched_id)
        return jsonify(result), 200 if result['ok'] else 500
    except Exception as e:
        logger.error(f"[/api/deals/run/{sched_id}] {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500


# ── Schedules ─────────────────────────────────────────────────────────────────

@deals_bp.route('/api/deals/schedules')
@login_required
def get_schedules():
    """Returns all scan schedules with their APScheduler job status."""
    conn = None
    try:
        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    id, name, trigger_type, cron_expression,
                    interval_hours, alert_threshold, filters, mode,
                    is_active, created_at, last_run_at
                FROM scan_schedules
                ORDER BY id
            """)
            rows = cur.fetchall()

        schedules = []
        for r in rows:
            job_info = get_job_status(r['id'])
            schedules.append({
                'id':              r['id'],
                'name':            r['name'],
                'trigger_type':    r['trigger_type'],
                'cron_expression': r['cron_expression'],
                'interval_hours':  r['interval_hours'],
                'alert_threshold': r['alert_threshold'],
                'filters':         r['filters'] or {},
                'mode':            r['mode'],
                'is_active':       r['is_active'],
                'last_run_at':     r['last_run_at'].isoformat() if r['last_run_at'] else None,
                'job':             job_info,
            })

        return jsonify({'ok': True, 'schedules': schedules})

    except Exception as e:
        logger.error(f"[/api/deals/schedules] {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


@deals_bp.route('/api/deals/schedules/<int:sched_id>', methods=['PUT'])
@login_required
def update_schedule(sched_id):
    """
    Pause or resume a schedule.
    Body: { "is_active": true/false }
    """
    conn = None
    try:
        data      = request.get_json(silent=True) or {}
        is_active = data.get('is_active')

        if is_active is None:
            return jsonify({'ok': False, 'error': 'is_active required'}), 400

        conn = get_db()
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE scan_schedules
                SET is_active = %s
                WHERE id = %s
                RETURNING id, name, is_active
            """, (is_active, sched_id))
            row = cur.fetchone()

        if not row:
            return jsonify({'ok': False, 'error': 'Schedule not found'}), 404

        conn.commit()

        # Sync APScheduler job state
        if is_active:
            resume_job(sched_id)
        else:
            pause_job(sched_id)

        return jsonify({
            'ok':       True,
            'id':       row['id'],
            'name':     row['name'],
            'is_active':row['is_active'],
        })

    except Exception as e:
        logger.error(f"[/api/deals/schedules/{sched_id}] {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()


# ── Status bar ────────────────────────────────────────────────────────────────

@deals_bp.route('/api/deals/status')
@login_required
def get_status():
    """
    Returns info for the Deal Blaster scan bar:
    - last run time
    - deals found in last run
    - active schedule count
    """
    conn = None
    try:
        conn = get_db()
        with conn.cursor() as cur:

            # Last successful run
            cur.execute("""
                SELECT run_at, deals_found, new_deals, triggered_by, duration_ms
                FROM scan_runs
                WHERE status = 'ok'
                ORDER BY run_at DESC
                LIMIT 1
            """)
            last_run = cur.fetchone()

            # Active deal count
            cur.execute("""
                SELECT COUNT(*) as cnt
                FROM bb_deals
                WHERE is_active = TRUE AND expires_at > NOW()
            """)
            deal_count = cur.fetchone()['cnt']

            # Active schedules
            cur.execute("""
                SELECT COUNT(*) as cnt
                FROM scan_schedules
                WHERE is_active = TRUE
            """)
            active_schedules = cur.fetchone()['cnt']

        return jsonify({
            'ok':              True,
            'deal_count':      deal_count,
            'active_schedules':active_schedules,
            'last_run':        {
                'run_at':       last_run['run_at'].isoformat() if last_run else None,
                'deals_found':  last_run['deals_found'] if last_run else 0,
                'new_deals':    last_run['new_deals'] if last_run else 0,
                'triggered_by': last_run['triggered_by'] if last_run else None,
                'duration_ms':  last_run['duration_ms'] if last_run else None,
            } if last_run else None,
        })

    except Exception as e:
        logger.error(f"[/api/deals/status] {e}")
        return jsonify({'ok': False, 'error': str(e)}), 500
    finally:
        if conn:
            conn.close()
