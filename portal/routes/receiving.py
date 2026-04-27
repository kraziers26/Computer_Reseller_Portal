from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required, current_user
from ..auth_utils import require_role
from ..db import db_cursor
import uuid

receiving_bp = Blueprint('receiving', __name__, url_prefix='/receiving')


# ── Session list ──────────────────────────────────────────────────────────────

@receiving_bp.route('/')
@login_required
@require_role('admin')
def index():
    with db_cursor() as (cur, _):
        # Batched transactions not yet received — grouped by batch
        cur.execute("""
            SELECT t.print_batch_id AS batch_id,
                   MIN(t.print_date)      AS batch_date,
                   COUNT(*)               AS total_orders,
                   COUNT(*) FILTER (WHERE t.fulfillment_status='batched')  AS pending_count,
                   COUNT(*) FILTER (WHERE t.fulfillment_status='received') AS received_count,
                   STRING_AGG(DISTINCT c.company_name, ', ') AS companies
            FROM transactions t
            LEFT JOIN dim_companies c ON t.company_id = c.company_id
            WHERE t.print_batch_id IS NOT NULL
              AND t.is_active = TRUE
              AND t.fulfillment_status IN ('batched','received')
            GROUP BY t.print_batch_id
            ORDER BY batch_date DESC
        """)
        batches = cur.fetchall()

        # Active receiving sessions
        cur.execute("""
            SELECT rs.session_id, rs.batch_id, rs.created_at, rs.status,
                   u.username AS created_by,
                   COUNT(ri.item_id) AS total_items,
                   COUNT(ri.item_id) FILTER (WHERE ri.receive_status='received') AS received_count,
                   COUNT(ri.item_id) FILTER (WHERE ri.receive_status='missing')  AS missing_count,
                   COUNT(ri.item_id) FILTER (WHERE ri.receive_status='partial')  AS partial_count,
                   COUNT(ri.item_id) FILTER (WHERE ri.receive_status='pending')  AS pending_count
            FROM receiving_sessions rs
            LEFT JOIN dim_users u     ON rs.created_by = u.user_id
            LEFT JOIN receiving_items ri ON rs.session_id = ri.session_id
            GROUP BY rs.session_id, rs.batch_id, rs.created_at, rs.status, u.username
            ORDER BY rs.created_at DESC
        """)
        sessions = cur.fetchall()

    return render_template('receiving/index.html', batches=batches, sessions=sessions)


# ── Start a new session ───────────────────────────────────────────────────────

@receiving_bp.route('/start', methods=['POST'])
@login_required
@require_role('admin')
def start_session():
    batch_id = request.form.get('batch_id', '').strip()
    if not batch_id:
        flash('Batch ID required.', 'error')
        return redirect(url_for('receiving.index'))

    with db_cursor() as (cur, conn):
        # Check no open session exists for this batch
        cur.execute(
            "SELECT session_id FROM receiving_sessions WHERE batch_id=%s AND status='open'",
            (batch_id,))
        existing = cur.fetchone()
        if existing:
            flash(f'An open session already exists for {batch_id}.', 'warning')
            return redirect(url_for('receiving.session', session_id=str(existing['session_id'])))

        # Create session
        session_id = str(uuid.uuid4())
        cur.execute(
            "INSERT INTO receiving_sessions (session_id, batch_id, created_by) VALUES (%s,%s,%s)",
            (session_id, batch_id, current_user.id))

        # Pull all batched transactions for this batch and create receiving_items
        cur.execute("""
            SELECT transaction_id FROM transactions
            WHERE print_batch_id=%s AND is_active=TRUE AND fulfillment_status='batched'
        """, (batch_id,))
        txns = cur.fetchall()
        for txn in txns:
            item_id = str(uuid.uuid4())
            cur.execute(
                "INSERT INTO receiving_items (item_id, session_id, transaction_id) VALUES (%s,%s,%s)",
                (item_id, session_id, str(txn['transaction_id'])))
            # Pre-populate line items from transaction_items
            cur.execute(
                "SELECT item_id AS transaction_item_id, quantity FROM transaction_items WHERE transaction_id=%s",
                (str(txn['transaction_id']),))
            items = cur.fetchall()
            for li in items:
                cur.execute(
                    "INSERT INTO receiving_item_lines (item_id, transaction_item_id, ordered_qty, received_qty) "
                    "VALUES (%s,%s,%s,%s)",
                    (item_id, str(li['transaction_item_id']), li['quantity'] or 0, 0))

    flash(f'Receiving session started for {batch_id}.', 'success')
    return redirect(url_for('receiving.session', session_id=session_id))


# ── Session detail ────────────────────────────────────────────────────────────

@receiving_bp.route('/session/<session_id>', methods=['GET', 'POST'])
@login_required
@require_role('admin')
def session(session_id):
    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT rs.*, u.username AS created_by_name
            FROM receiving_sessions rs
            LEFT JOIN dim_users u ON rs.created_by = u.user_id
            WHERE rs.session_id = %s
        """, (session_id,))
        sess = cur.fetchone()
        if not sess:
            flash('Session not found.', 'error')
            return redirect(url_for('receiving.index'))

        cur.execute("""
            SELECT ri.item_id, ri.receive_status, ri.notes,
                   t.transaction_id, t.order_number, t.retailer, t.purchase_date,
                   t.price_total, u.username AS person_name, c.company_name,
                   t.fulfillment_status,
                   EXTRACT(EPOCH FROM (NOW() - COALESCE(t.fulfillment_status_updated_at,
                           t.submitted_at))) / 86400 AS days_in_status
            FROM receiving_items ri
            JOIN transactions t     ON ri.transaction_id = t.transaction_id
            LEFT JOIN dim_users u   ON t.user_id = u.user_id
            LEFT JOIN dim_companies c ON t.company_id = c.company_id
            WHERE ri.session_id = %s
            ORDER BY t.retailer, t.order_number
        """, (session_id,))
        items = cur.fetchall()

        # Load line items for each receiving_item
        line_map = {}
        for item in items:
            cur.execute("""
                SELECT ril.line_id, ril.ordered_qty, ril.received_qty,
                       ti.item_description, ti.sku_model_color, ti.quantity
                FROM receiving_item_lines ril
                JOIN transaction_items ti ON ril.transaction_item_id = ti.item_id
                WHERE ril.item_id = %s
            """, (str(item['item_id']),))
            line_map[str(item['item_id'])] = cur.fetchall()

        # Summary counts
        total   = len(items)
        rcvd    = sum(1 for i in items if i['receive_status'] == 'received')
        missing = sum(1 for i in items if i['receive_status'] == 'missing')
        partial = sum(1 for i in items if i['receive_status'] == 'partial')
        pending = sum(1 for i in items if i['receive_status'] == 'pending')

    return render_template('receiving/session.html',
                           sess=sess, items=items, line_map=line_map,
                           total=total, rcvd=rcvd, missing=missing,
                           partial=partial, pending=pending,
                           session_id=session_id)


# ── Mark item status (AJAX) ───────────────────────────────────────────────────

@receiving_bp.route('/session/<session_id>/mark', methods=['POST'])
@login_required
@require_role('admin')
def mark_item(session_id):
    item_id       = request.json.get('item_id')
    status        = request.json.get('status')   # received / missing / partial / pending
    line_qtys     = request.json.get('lines', {}) # {line_id: received_qty} for partial
    notes         = request.json.get('notes', '')

    if status not in ('received', 'missing', 'partial', 'pending'):
        return jsonify({'error': 'Invalid status'}), 400
    # 'pending' = goes to pending pool when session closes

    with db_cursor() as (cur, conn):
        cur.execute(
            "UPDATE receiving_items SET receive_status=%s, notes=%s, updated_at=NOW() "
            "WHERE item_id=%s AND session_id=%s",
            (status, notes or None, item_id, session_id))

        if status == 'partial' and line_qtys:
            for line_id, qty in line_qtys.items():
                cur.execute(
                    "UPDATE receiving_item_lines SET received_qty=%s WHERE line_id=%s",
                    (int(qty), line_id))
        elif status == 'received':
            # Mark all lines as fully received
            cur.execute(
                "UPDATE receiving_item_lines SET received_qty=ordered_qty "
                "WHERE item_id=%s", (item_id,))
        elif status == 'missing':
            # Zero out received quantities
            cur.execute(
                "UPDATE receiving_item_lines SET received_qty=0 WHERE item_id=%s",
                (item_id,))

        # If fully received, update fulfillment_status on the transaction
        if status == 'received':
            cur.execute(
                "UPDATE transactions SET fulfillment_status='received', "
                "fulfillment_status_updated_at=NOW() "
                "WHERE transaction_id=("
                "SELECT transaction_id FROM receiving_items WHERE item_id=%s)",
                (item_id,))

    return jsonify({'ok': True})


# ── Close session ─────────────────────────────────────────────────────────────

@receiving_bp.route('/session/<session_id>/close', methods=['POST'])
@login_required
@require_role('admin')
def close_session(session_id):
    with db_cursor() as (cur, conn):
        # Move missing/partial items to new pending pool (remove from batch)
        cur.execute("""
            UPDATE transactions t
            SET fulfillment_status='batched'
            FROM receiving_items ri
            WHERE ri.transaction_id = t.transaction_id
              AND ri.session_id = %s
              AND ri.receive_status IN ('missing','partial','pending')
        """, (session_id,))
        cur.execute(
            "UPDATE receiving_sessions SET status='closed' WHERE session_id=%s",
            (session_id,))

    flash('Session closed. Missing and partial items remain in the receiving pool.', 'success')
    return redirect(url_for('receiving.index'))


# ── Pending pool ──────────────────────────────────────────────────────────────

@receiving_bp.route('/pending-pool')
@login_required
@require_role('admin')
def pending_pool():
    f_status     = request.args.get('status', '')
    f_retailer   = request.args.get('retailer', '')
    f_company    = request.args.get('company', type=int)
    f_person     = request.args.get('person_by', type=int)
    f_order      = request.args.get('order_number', '')
    f_stuck_days = request.args.get('stuck_days', type=int)
    f_batch      = request.args.get('batch_id', '')

    conditions = [
        "ri.receive_status IN ('pending', 'missing', 'partial')",
        "rs.status = 'closed'",
        "t.is_active = TRUE"
    ]
    params = []
    if f_status:   conditions.append("ri.receive_status = %s"); params.append(f_status)
    if f_retailer: conditions.append("t.retailer = %s"); params.append(f_retailer)
    if f_company:  conditions.append("t.company_id = %s"); params.append(f_company)
    if f_person:   conditions.append("t.user_id = %s"); params.append(f_person)
    if f_order:    conditions.append("t.order_number ILIKE %s"); params.append(f'%{f_order}%')
    if f_batch:    conditions.append("rs.batch_id ILIKE %s"); params.append(f'%{f_batch}%')
    if f_stuck_days:
        conditions.append(
            "EXTRACT(EPOCH FROM (NOW()-COALESCE(t.fulfillment_status_updated_at,t.submitted_at)))/86400 >= %s")
        params.append(f_stuck_days)
    where = 'WHERE ' + ' AND '.join(conditions)

    with db_cursor() as (cur, _):
        cur.execute(f"""
            SELECT ri.item_id, ri.receive_status, ri.notes,
                   rs.batch_id, rs.session_id,
                   t.transaction_id, t.order_number, t.retailer,
                   t.purchase_date, t.price_total, t.order_type,
                   t.fulfillment_status, t.fulfillment_status_updated_at,
                   t.review_status, t.card_id,
                   ROUND(EXTRACT(EPOCH FROM (NOW()-COALESCE(t.fulfillment_status_updated_at,
                         t.submitted_at)))/86400) AS days_in_status,
                   u.username AS person_name, c.company_name,
                   sub.username AS submitter_name
            FROM receiving_items ri
            JOIN receiving_sessions rs ON ri.session_id = rs.session_id
            JOIN transactions t        ON ri.transaction_id = t.transaction_id
            LEFT JOIN dim_users u      ON t.user_id = u.user_id
            LEFT JOIN dim_users sub    ON t.submitted_by_email = sub.email
            LEFT JOIN dim_companies c  ON t.company_id = c.company_id
            {where}
            ORDER BY ri.receive_status, days_in_status DESC
        """, params)
        items = cur.fetchall()

        cur.execute("SELECT DISTINCT retailer FROM transactions WHERE is_active=TRUE ORDER BY retailer")
        retailers = [r['retailer'] for r in cur.fetchall()]
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active=TRUE ORDER BY username")
        users = cur.fetchall()
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE ORDER BY company_name")
        companies = cur.fetchall()

        # Load line items for all displayed orders (>= $1 only)
        order_items = {}
        if items:
            txn_ids = [str(r['transaction_id']) for r in items]
            cur.execute("""
                SELECT transaction_id, item_description, sku_model_color,
                       quantity, unit_price, line_total
                FROM transaction_items
                WHERE transaction_id = ANY(%s::uuid[])
                  AND unit_price >= 1.0
                ORDER BY transaction_id, unit_price DESC
            """, (txn_ids,))
            for ti in cur.fetchall():
                tid = str(ti['transaction_id'])
                order_items.setdefault(tid, []).append(ti)

    return render_template('receiving/pending_pool.html', items=items,
                           order_items=order_items,
                           retailers=retailers, users=users, companies=companies,
                           filters={'status':f_status,'retailer':f_retailer,'company':f_company,
                                    'person_by':f_person,'order_number':f_order,
                                    'stuck_days':f_stuck_days,'batch_id':f_batch})


@receiving_bp.route('/pending-pool/move-received', methods=['POST'])
@login_required
@require_role('admin')
def move_to_received():
    item_ids = request.form.getlist('item_ids')
    if not item_ids:
        flash('No items selected.', 'error')
        return redirect(url_for('receiving.pending_pool'))
    with db_cursor() as (cur, conn):
        cur.execute(
            "UPDATE receiving_items SET receive_status='received', updated_at=NOW() "
            "WHERE item_id = ANY(%s::uuid[])", (item_ids,))
        # Update fulfillment_status on transactions
        cur.execute("""
            UPDATE transactions t
            SET fulfillment_status='received', fulfillment_status_updated_at=NOW()
            FROM receiving_items ri
            WHERE ri.transaction_id = t.transaction_id
              AND ri.item_id = ANY(%s::uuid[])
        """, (item_ids,))
    flash(f'{len(item_ids)} order(s) moved to Received.', 'success')
    return redirect(url_for('receiving.pending_pool'))
