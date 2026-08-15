import os, sys, uuid, json
from datetime import datetime
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, send_file
from flask_login import login_required, current_user
from ..auth_utils import require_role
from ..db import db_cursor

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'parsers'))

sales_tracker_bp = Blueprint('sales_tracker', __name__, url_prefix='/sales')
ALLOWED_EXT = {'pdf'}
UPLOAD_FOLDER = '/tmp/portal_uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


def _to_iso_date(us_date):
    """Parser returns dates as 'M/D/YYYY' (as printed on the invoice); DB/inputs need ISO."""
    if not us_date:
        return None
    try:
        return datetime.strptime(us_date, '%m/%d/%Y').strftime('%Y-%m-%d')
    except ValueError:
        return None


def _run_parser(pdf_path):
    from sales_parser import parse_sales_invoice
    return parse_sales_invoice(pdf_path)


def _commit_sales_invoice(cur, parsed, form, dup_invoice, source_filename):
    """Shared save logic for both the single-file confirm flow and batch
    item confirm — resolves/creates the client, supersedes the previous
    invoice version if replacing, and inserts the invoice + line items.
    Returns the new sales_invoice_id."""
    client_name = form.get('client_name', '').strip()
    client_id = form.get('client_id', type=int)
    company_id = form.get('company_id', type=int)
    commission_amount = form.get('commission_amount', type=float)
    total = form.get('total', type=float)
    invoice_date = form.get('invoice_date')
    submitted_by = form.get('user_id', type=int) or current_user.id

    if not client_id:
        cur.execute("SELECT client_id FROM dim_sales_clients WHERE LOWER(client_name) = LOWER(%s)",
                    (client_name,))
        row = cur.fetchone()
        if row:
            client_id = row['client_id']
        else:
            cur.execute("""
                INSERT INTO dim_sales_clients (client_name, address, phone)
                VALUES (%s, %s, %s) RETURNING client_id
            """, (client_name, parsed.get('client_address'), parsed.get('client_phone')))
            client_id = cur.fetchone()['client_id']

    if dup_invoice:
        cur.execute("""
            UPDATE sales_invoices SET is_superseded = TRUE
            WHERE invoice_number = %s AND is_superseded = FALSE
        """, (parsed['invoice_number'],))

    new_id = str(uuid.uuid4())
    cur.execute("""
        INSERT INTO sales_invoices
            (sales_invoice_id, invoice_number, invoice_date, client_id,
             total, commission_amount, company_id, source_file_path, submitted_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (new_id, parsed['invoice_number'], invoice_date, client_id,
          total, commission_amount, company_id, source_filename, submitted_by))

    descs   = form.getlist('item_description[]')
    codes   = form.getlist('item_code[]')
    qtys    = form.getlist('item_qty[]')
    prices  = form.getlist('item_price[]')
    amounts = form.getlist('item_amount[]')
    types   = form.getlist('item_type[]')
    customs = set(form.getlist('item_customs[]'))  # indices of checked boxes

    for i, desc in enumerate(descs):
        if not desc.strip():
            continue
        cur.execute("""
            INSERT INTO sales_invoice_items
                (sales_invoice_id, item_code, description, product_type,
                 quantity, price_each, amount, is_customs_only)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (new_id, codes[i], desc.strip(), types[i] or 'other',
              int(qtys[i] or 0), float(prices[i] or 0),
              float(amounts[i] or 0), str(i) in customs))

    return new_id


# ── Overview ─────────────────────────────────────────────────────────────────

@sales_tracker_bp.route('/')
@login_required
@require_role('contributor')
def overview():
    f_salesman = request.args.get('salesman', type=int)
    f_year     = request.args.get('year', type=int)
    f_month    = request.args.get('month', type=int)
    f_company  = request.args.get('company', type=int)
    f_client   = request.args.get('client', type=int)

    conditions = ["i.is_superseded = FALSE"]
    params = []
    if f_salesman:
        conditions.append("i.submitted_by = %s"); params.append(f_salesman)
    if f_year:
        conditions.append("EXTRACT(YEAR FROM i.invoice_date) = %s"); params.append(f_year)
    if f_month:
        conditions.append("EXTRACT(MONTH FROM i.invoice_date) = %s"); params.append(f_month)
    if f_company:
        conditions.append("i.company_id = %s"); params.append(f_company)
    if f_client:
        conditions.append("i.client_id = %s"); params.append(f_client)
    where = " AND ".join(conditions)

    with db_cursor() as (cur, _):
        # Filter option lists
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active = TRUE ORDER BY username")
        salesmen = cur.fetchall()
        cur.execute("""
            SELECT DISTINCT EXTRACT(YEAR FROM invoice_date)::int AS year
            FROM sales_invoices WHERE is_superseded = FALSE ORDER BY year DESC
        """)
        years = [r['year'] for r in cur.fetchall()]
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active = TRUE ORDER BY company_name")
        companies = cur.fetchall()
        cur.execute("SELECT client_id, client_name FROM dim_sales_clients ORDER BY client_name")
        all_clients = cur.fetchall()

        # KPIs
        cur.execute(f"""
            SELECT COALESCE(SUM(i.commission_amount), 0) AS total_commission,
                   COUNT(*)                              AS number_of_sales,
                   COALESCE(SUM(i.total), 0)              AS total_volume,
                   COUNT(DISTINCT i.client_id)            AS distinct_clients
            FROM sales_invoices i WHERE {where}
        """, params)
        kpis = cur.fetchone()

        # Commission by month
        cur.execute(f"""
            SELECT TO_CHAR(i.invoice_date, 'Mon YYYY') AS month,
                   SUM(i.commission_amount) AS commission,
                   MIN(DATE_TRUNC('month', i.invoice_date)) AS sort_key
            FROM sales_invoices i WHERE {where}
            GROUP BY TO_CHAR(i.invoice_date, 'Mon YYYY')
            ORDER BY sort_key
        """, params)
        by_month = cur.fetchall()
        max_month_commission = max([float(r['commission'] or 0) for r in by_month], default=1) or 1

        # Top clients, with account owner ("whose client is it")
        cur.execute(f"""
            SELECT c.client_id, c.client_name, u.username AS owner_name,
                   COUNT(i.sales_invoice_id) AS order_count,
                   SUM(i.total) AS total_revenue
            FROM sales_invoices i
            JOIN dim_sales_clients c ON i.client_id = c.client_id
            LEFT JOIN dim_users u ON c.account_owner_user_id = u.user_id
            WHERE {where}
            GROUP BY c.client_id, c.client_name, u.username
            ORDER BY total_revenue DESC LIMIT 5
        """, params)
        top_clients = cur.fetchall()

        # Top salesmen (Person By)
        cur.execute(f"""
            SELECT u.user_id, u.username,
                   COUNT(i.sales_invoice_id) AS order_count,
                   COALESCE(SUM(i.total), 0) AS total_revenue,
                   COALESCE(SUM(i.commission_amount), 0) AS total_commission
            FROM sales_invoices i
            JOIN dim_users u ON i.submitted_by = u.user_id
            WHERE {where}
            GROUP BY u.user_id, u.username
            ORDER BY total_commission DESC LIMIT 5
        """, params)
        top_salesmen = cur.fetchall()

        cur.execute(f"""
            SELECT li.item_code, MAX(li.description) AS description,
                   MAX(li.product_type) AS product_type,
                   SUM(li.quantity) AS units_sold, SUM(li.amount) AS total_revenue
            FROM sales_invoice_items li
            JOIN sales_invoices i ON li.sales_invoice_id = i.sales_invoice_id
            WHERE {where} AND li.is_customs_only = FALSE
            GROUP BY li.item_code ORDER BY total_revenue DESC LIMIT 5
        """, params)
        top_products = cur.fetchall()

        cur.execute(f"""
            SELECT COALESCE(li.product_type, 'other') AS product_type,
                   SUM(li.quantity) AS units
            FROM sales_invoice_items li
            JOIN sales_invoices i ON li.sales_invoice_id = i.sales_invoice_id
            WHERE {where} AND li.is_customs_only = FALSE
            GROUP BY COALESCE(li.product_type, 'other')
            ORDER BY units DESC
        """, params)
        mix_rows = cur.fetchall()
        mix_total = sum(r['units'] for r in mix_rows) or 1

        cur.execute(f"""
            SELECT i.sales_invoice_id, i.invoice_number, i.invoice_date, i.total,
                   i.commission_amount, c.client_name, u.username AS submitted_by_name,
                   (SELECT COUNT(*) FROM sales_invoices h
                    WHERE h.invoice_number = i.invoice_number) AS version_count
            FROM sales_invoices i
            JOIN dim_sales_clients c ON i.client_id = c.client_id
            LEFT JOIN dim_users u ON i.submitted_by = u.user_id
            WHERE {where}
            ORDER BY i.uploaded_at DESC LIMIT 10
        """, params)
        recent_invoices = cur.fetchall()

    return render_template('sales_tracker/overview.html',
        kpis=kpis, by_month=by_month, max_month_commission=max_month_commission,
        top_clients=top_clients, top_salesmen=top_salesmen, top_products=top_products,
        mix_rows=mix_rows, mix_total=mix_total, recent_invoices=recent_invoices,
        salesmen=salesmen, years=years, companies=companies, all_clients=all_clients,
        filters={'salesman': f_salesman, 'year': f_year, 'month': f_month,
                 'company': f_company, 'client': f_client})


# ── Invoices ─────────────────────────────────────────────────────────────────

@sales_tracker_bp.route('/invoices')
@login_required
@require_role('contributor')
def invoices():
    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT i.sales_invoice_id, i.invoice_number, i.invoice_date, i.total,
                   i.commission_amount, i.company_id, comp.company_name,
                   c.client_name, u.username AS submitted_by_name,
                   (SELECT COUNT(*) FROM sales_invoices h
                    WHERE h.invoice_number = i.invoice_number) AS version_count
            FROM sales_invoices i
            JOIN dim_sales_clients c ON i.client_id = c.client_id
            LEFT JOIN dim_companies comp ON i.company_id = comp.company_id
            LEFT JOIN dim_users u ON i.submitted_by = u.user_id
            WHERE i.is_superseded = FALSE
            ORDER BY i.invoice_date DESC
        """)
        invoice_rows = cur.fetchall()
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active = TRUE ORDER BY company_name")
        companies = cur.fetchall()
    return render_template('sales_tracker/invoices.html', invoices=invoice_rows, companies=companies)


@sales_tracker_bp.route('/invoices/<uuid:invoice_id>/commission', methods=['POST'])
@login_required
@require_role('contributor')
def update_commission(invoice_id):
    amount = request.form.get('commission_amount', type=float)
    company_id = request.form.get('company_id', type=int)
    with db_cursor() as (cur, _):
        cur.execute("""
            UPDATE sales_invoices SET commission_amount = %s, company_id = %s
            WHERE sales_invoice_id = %s
        """, (amount, company_id, str(invoice_id)))
    flash('Commission updated.', 'success')
    return redirect(url_for('sales_tracker.invoices'))


@sales_tracker_bp.route('/invoices/<uuid:invoice_id>/delete', methods=['POST'])
@login_required
@require_role('contributor')
def delete_invoice(invoice_id):
    with db_cursor() as (cur, conn):
        cur.execute("SELECT invoice_number FROM sales_invoices WHERE sales_invoice_id = %s", (str(invoice_id),))
        row = cur.fetchone()
        if not row:
            flash('Invoice not found.', 'error')
            return redirect(url_for('sales_tracker.invoices'))
        # Delete every stored version of this invoice number (active + any
        # superseded history), not just the row that was clicked — deleting
        # from the list means "this invoice shouldn't be on record", not
        # "hide the latest snapshot but keep stale duplicates around".
        cur.execute("DELETE FROM sales_invoices WHERE invoice_number = %s", (row['invoice_number'],))
    flash(f"Invoice #{row['invoice_number']} deleted.", 'success')
    return redirect(url_for('sales_tracker.invoices'))


@sales_tracker_bp.route('/invoices/<uuid:invoice_id>/edit', methods=['GET', 'POST'])
@login_required
@require_role('contributor')
def edit_invoice(invoice_id):
    with db_cursor() as (cur, _):
        cur.execute("SELECT * FROM sales_invoices WHERE sales_invoice_id = %s", (str(invoice_id),))
        invoice = cur.fetchone()
        if not invoice:
            flash('Invoice not found.', 'error')
            return redirect(url_for('sales_tracker.invoices'))

        cur.execute("SELECT * FROM sales_invoice_items WHERE sales_invoice_id = %s", (str(invoice_id),))
        items = cur.fetchall()
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active = TRUE ORDER BY company_name")
        companies = cur.fetchall()
        cur.execute("SELECT client_id, client_name FROM dim_sales_clients ORDER BY client_name")
        existing_clients = cur.fetchall()
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active = TRUE ORDER BY username")
        users = cur.fetchall()

    if request.method == 'POST':
        client_id = request.form.get('client_id', type=int)
        company_id = request.form.get('company_id', type=int)
        commission_amount = request.form.get('commission_amount', type=float)
        total = request.form.get('total', type=float)
        invoice_date = request.form.get('invoice_date')
        submitted_by = request.form.get('user_id', type=int)

        with db_cursor() as (cur, conn):
            cur.execute("""
                UPDATE sales_invoices
                SET invoice_date = %s, client_id = %s, total = %s,
                    commission_amount = %s, company_id = %s, submitted_by = %s
                WHERE sales_invoice_id = %s
            """, (invoice_date, client_id, total, commission_amount, company_id,
                  submitted_by, str(invoice_id)))

            # Line items are fully replaced on save, same convention as the
            # existing Invoicing module's edit flow.
            cur.execute("DELETE FROM sales_invoice_items WHERE sales_invoice_id = %s", (str(invoice_id),))

            descs   = request.form.getlist('item_description[]')
            codes   = request.form.getlist('item_code[]')
            qtys    = request.form.getlist('item_qty[]')
            prices  = request.form.getlist('item_price[]')
            amounts = request.form.getlist('item_amount[]')
            types   = request.form.getlist('item_type[]')
            customs = set(request.form.getlist('item_customs[]'))

            for i, desc in enumerate(descs):
                if not desc.strip():
                    continue
                cur.execute("""
                    INSERT INTO sales_invoice_items
                        (sales_invoice_id, item_code, description, product_type,
                         quantity, price_each, amount, is_customs_only)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (str(invoice_id), codes[i], desc.strip(), types[i] or 'other',
                      int(qtys[i] or 0), float(prices[i] or 0),
                      float(amounts[i] or 0), str(i) in customs))

        flash(f"Invoice #{invoice['invoice_number']} updated.", 'success')
        return redirect(url_for('sales_tracker.invoices'))

    return render_template('sales_tracker/edit.html', invoice=invoice, items=items,
        companies=companies, existing_clients=existing_clients, users=users,
        iso_date=invoice['invoice_date'].isoformat() if invoice['invoice_date'] else '')


# ── Clients ──────────────────────────────────────────────────────────────────

@sales_tracker_bp.route('/clients')
@login_required
@require_role('contributor')
def clients():
    with db_cursor() as (cur, _):
        # LEFT JOIN (not the v_sales_by_client view, which inner-joins and
        # would hide newly-added clients with zero orders yet)
        cur.execute("""
            SELECT c.client_id, c.client_name, c.account_owner_user_id,
                   u.username AS owner_name,
                   COUNT(i.sales_invoice_id) FILTER (WHERE i.is_superseded = FALSE) AS order_count,
                   COALESCE(SUM(i.total) FILTER (WHERE i.is_superseded = FALSE), 0) AS total_revenue,
                   COALESCE(SUM(i.commission_amount) FILTER (WHERE i.is_superseded = FALSE), 0) AS total_commission,
                   MAX(i.invoice_date) FILTER (WHERE i.is_superseded = FALSE) AS last_order_date
            FROM dim_sales_clients c
            LEFT JOIN dim_users u ON c.account_owner_user_id = u.user_id
            LEFT JOIN sales_invoices i ON i.client_id = c.client_id
            GROUP BY c.client_id, c.client_name, c.account_owner_user_id, u.username
            ORDER BY total_revenue DESC, c.client_name
        """)
        client_rows = cur.fetchall()
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active = TRUE ORDER BY username")
        salesmen = cur.fetchall()
    return render_template('sales_tracker/clients.html', clients=client_rows, salesmen=salesmen)


@sales_tracker_bp.route('/clients/<int:client_id>/owner', methods=['POST'])
@login_required
@require_role('contributor')
def update_client_owner(client_id):
    owner_id = request.form.get('account_owner_user_id', type=int)
    with db_cursor() as (cur, _):
        cur.execute("UPDATE dim_sales_clients SET account_owner_user_id = %s WHERE client_id = %s",
                    (owner_id, client_id))
    flash('Client owner updated.', 'success')
    return redirect(url_for('sales_tracker.clients'))


@sales_tracker_bp.route('/clients/new', methods=['GET', 'POST'])
@login_required
@require_role('contributor')
def new_client():
    with db_cursor() as (cur, _):
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active = TRUE ORDER BY username")
        salesmen = cur.fetchall()

    if request.method == 'POST':
        client_name = request.form.get('client_name', '').strip()
        address = request.form.get('address', '').strip() or None
        phone = request.form.get('phone', '').strip() or None
        owner_id = request.form.get('account_owner_user_id', type=int)

        if not client_name:
            flash('Client name is required.', 'error')
            return render_template('sales_tracker/new_client.html', salesmen=salesmen)

        with db_cursor() as (cur, conn):
            cur.execute("SELECT client_id FROM dim_sales_clients WHERE LOWER(client_name) = LOWER(%s)",
                        (client_name,))
            if cur.fetchone():
                flash(f'A client named "{client_name}" already exists.', 'error')
                return render_template('sales_tracker/new_client.html', salesmen=salesmen,
                    client_name=client_name, address=address, phone=phone)

            cur.execute("""
                INSERT INTO dim_sales_clients (client_name, address, phone, account_owner_user_id)
                VALUES (%s, %s, %s, %s)
            """, (client_name, address, phone, owner_id))

        flash(f'Client "{client_name}" added.', 'success')
        return redirect(url_for('sales_tracker.clients'))

    return render_template('sales_tracker/new_client.html', salesmen=salesmen)


# ── Upload (single file → session flow, multiple files → batch draft) ────────

@sales_tracker_bp.route('/upload', methods=['GET', 'POST'])
@login_required
@require_role('contributor')
def upload():
    active_draft = None
    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT draft_id, total_files, completed_count, created_at
            FROM sales_batch_drafts
            WHERE user_id = %s AND status = 'active'
            ORDER BY created_at DESC LIMIT 1
        """, (current_user.id,))
        active_draft = cur.fetchone()

    if request.method == 'POST':
        files = request.files.getlist('pdfs')
        valid = [f for f in files if f.filename and allowed_file(f.filename)]

        if not valid:
            flash('Please select at least one PDF file.', 'error')
            return render_template('sales_tracker/upload.html', active_draft=active_draft)

        # SINGLE FILE → straight to confirm (existing session-based flow)
        if len(valid) == 1:
            f = valid[0]
            tmp_path = os.path.join(UPLOAD_FOLDER, f'{uuid.uuid4()}.pdf')
            f.save(tmp_path)
            try:
                parsed = _run_parser(tmp_path)
            except Exception as e:
                os.remove(tmp_path)
                flash(f'Could not parse this PDF: {e}', 'error')
                return render_template('sales_tracker/upload.html', active_draft=active_draft)

            if not parsed.get('invoice_number') or not parsed.get('line_items'):
                os.remove(tmp_path)
                flash('Could not find an invoice number or line items on this PDF. '
                      'Double check this is a Sunrise-format invoice.', 'error')
                return render_template('sales_tracker/upload.html', active_draft=active_draft)

            parsed['_tmp_path'] = tmp_path
            parsed['_source_filename'] = f.filename
            session['pending_sales_invoice'] = json.dumps(parsed)
            return redirect(url_for('sales_tracker.confirm'))

        # MULTIPLE FILES → batch draft, same pattern as the Inventory upload
        draft_id = str(uuid.uuid4())
        with db_cursor() as (cur, conn):
            cur.execute("""
                INSERT INTO sales_batch_drafts (draft_id, user_id, total_files)
                VALUES (%s, %s, %s)
            """, (draft_id, current_user.id, len(valid)))

            for pos, f in enumerate(valid):
                tmp_path = os.path.join(UPLOAD_FOLDER, f'{uuid.uuid4()}.pdf')
                f.save(tmp_path)
                pdf_bytes = open(tmp_path, 'rb').read()
                try:
                    parsed = _run_parser(tmp_path)
                    ok = bool(parsed.get('invoice_number') and parsed.get('line_items'))
                except Exception:
                    parsed, ok = None, False
                os.remove(tmp_path)

                if ok:
                    cur.execute("""
                        INSERT INTO sales_batch_draft_items
                            (draft_id, position, filename, parse_status, invoice_data, pdf_bytes)
                        VALUES (%s, %s, %s, 'parsed', %s, %s)
                    """, (draft_id, pos, f.filename, json.dumps(parsed), pdf_bytes))
                else:
                    cur.execute("""
                        INSERT INTO sales_batch_draft_items
                            (draft_id, position, filename, parse_status, error_message, pdf_bytes)
                        VALUES (%s, %s, %s, 'failed', 'Could not parse this PDF', %s)
                    """, (draft_id, pos, f.filename, pdf_bytes))

        flash(f'Batch of {len(valid)} files created.', 'success')
        return redirect(url_for('sales_tracker.batch_review', draft_id=draft_id))

    return render_template('sales_tracker/upload.html', active_draft=active_draft)


@sales_tracker_bp.route('/upload/confirm', methods=['GET', 'POST'])
@login_required
@require_role('contributor')
def confirm():
    if 'pending_sales_invoice' not in session:
        return redirect(url_for('sales_tracker.upload'))
    parsed = json.loads(session['pending_sales_invoice'])

    with db_cursor() as (cur, _):
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active = TRUE ORDER BY company_name")
        companies = cur.fetchall()
        cur.execute("SELECT client_id, client_name FROM dim_sales_clients ORDER BY client_name")
        existing_clients = cur.fetchall()
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active = TRUE ORDER BY username")
        users = cur.fetchall()
        cur.execute("""
            SELECT sales_invoice_id, invoice_date, total, uploaded_at
            FROM sales_invoices WHERE invoice_number = %s AND is_superseded = FALSE
        """, (parsed['invoice_number'],))
        dup_invoice = cur.fetchone()

    ctx = dict(parsed=parsed, companies=companies, existing_clients=existing_clients,
               users=users, current_user_id=current_user.id,
               dup_invoice=dup_invoice,
               default_commission=round((parsed.get('computed_total') or 0) * 0.01, 2),
               iso_date=_to_iso_date(parsed.get('invoice_date')),
               form_action=url_for('sales_tracker.confirm'), batch=None)

    if request.method == 'POST':
        confirm_replace = request.form.get('confirm_replace') == 'yes'
        if dup_invoice and not confirm_replace:
            flash(f"Invoice #{parsed['invoice_number']} already exists "
                  f"(uploaded {dup_invoice['uploaded_at']:%b %d, %Y}, total "
                  f"${dup_invoice['total']:,.2f}). Review the differences below and "
                  f"confirm to replace it.", 'error')
            return render_template('sales_tracker/confirm.html', **ctx)

        tmp_path = parsed.get('_tmp_path')
        with db_cursor() as (cur, conn):
            _commit_sales_invoice(cur, parsed, request.form, dup_invoice, parsed.get('_source_filename'))

        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
        session.pop('pending_sales_invoice', None)
        flash(f"Invoice #{parsed['invoice_number']} saved.", 'success')
        return redirect(url_for('sales_tracker.invoices'))

    return render_template('sales_tracker/confirm.html', **ctx)


# ── Batch review (multi-file uploads) ────────────────────────────────────────

@sales_tracker_bp.route('/upload/batch/<draft_id>')
@login_required
@require_role('contributor')
def batch_review(draft_id):
    with db_cursor() as (cur, _):
        cur.execute("SELECT * FROM sales_batch_drafts WHERE draft_id = %s AND user_id = %s",
                    (draft_id, current_user.id))
        draft = cur.fetchone()
        if not draft:
            flash('Batch not found.', 'error')
            return redirect(url_for('sales_tracker.upload'))

        cur.execute("""
            SELECT * FROM sales_batch_draft_items WHERE draft_id = %s ORDER BY position
        """, (draft_id,))
        items = cur.fetchall()

    next_item = next((i for i in items if i['parse_status'] == 'parsed'), None)
    return render_template('sales_tracker/batch_review.html', draft=draft, items=items, next_item=next_item)


@sales_tracker_bp.route('/upload/batch/<draft_id>/item/<item_id>', methods=['GET', 'POST'])
@login_required
@require_role('contributor')
def batch_item_confirm(draft_id, item_id):
    with db_cursor() as (cur, _):
        cur.execute("SELECT * FROM sales_batch_drafts WHERE draft_id = %s AND user_id = %s",
                    (draft_id, current_user.id))
        draft = cur.fetchone()
        if not draft:
            flash('Batch not found.', 'error')
            return redirect(url_for('sales_tracker.upload'))

        cur.execute("SELECT * FROM sales_batch_draft_items WHERE item_id = %s AND draft_id = %s",
                    (item_id, draft_id))
        item = cur.fetchone()
        if not item:
            flash('Item not found.', 'error')
            return redirect(url_for('sales_tracker.batch_review', draft_id=draft_id))

        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active = TRUE ORDER BY company_name")
        companies = cur.fetchall()
        cur.execute("SELECT client_id, client_name FROM dim_sales_clients ORDER BY client_name")
        existing_clients = cur.fetchall()
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active = TRUE ORDER BY username")
        users = cur.fetchall()

        cur.execute("""
            SELECT COUNT(*) AS n FROM sales_batch_draft_items
            WHERE draft_id = %s AND parse_status = 'parsed'
        """, (draft_id,))
        remaining = cur.fetchone()['n']

    parsed = item['invoice_data']
    if isinstance(parsed, str):
        parsed = json.loads(parsed)

    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'skip':
            with db_cursor() as (cur, conn):
                cur.execute("UPDATE sales_batch_draft_items SET parse_status = 'skipped' WHERE item_id = %s",
                            (item_id,))
            flash('Invoice skipped.', 'info')
            return redirect(url_for('sales_tracker.batch_review', draft_id=draft_id))

        # action == 'submit'
        with db_cursor() as (cur, _):
            cur.execute("""
                SELECT sales_invoice_id, invoice_date, total, uploaded_at
                FROM sales_invoices WHERE invoice_number = %s AND is_superseded = FALSE
            """, (parsed['invoice_number'],))
            dup_invoice = cur.fetchone()

        confirm_replace = request.form.get('confirm_replace') == 'yes'
        if dup_invoice and not confirm_replace:
            flash(f"Invoice #{parsed['invoice_number']} already exists — "
                  f"confirm to replace it.", 'error')
            return render_template('sales_tracker/batch_item_confirm.html',
                draft=draft, item=item, parsed=parsed, companies=companies,
                existing_clients=existing_clients, users=users, current_user_id=current_user.id,
                dup_invoice=dup_invoice,
                default_commission=round((parsed.get('computed_total') or 0) * 0.01, 2),
                iso_date=_to_iso_date(parsed.get('invoice_date')), remaining=remaining)

        with db_cursor() as (cur, conn):
            new_id = _commit_sales_invoice(cur, parsed, request.form, dup_invoice, item['filename'])
            cur.execute("""
                UPDATE sales_batch_draft_items
                SET parse_status = 'submitted', sales_invoice_id = %s, submitted_at = NOW()
                WHERE item_id = %s
            """, (new_id, item_id))
            cur.execute("""
                UPDATE sales_batch_drafts SET completed_count = completed_count + 1, updated_at = NOW()
                WHERE draft_id = %s
            """, (draft_id,))

        flash(f"Invoice #{parsed['invoice_number']} saved.", 'success')

        with db_cursor() as (cur, _):
            cur.execute("""
                SELECT COUNT(*) AS n FROM sales_batch_draft_items
                WHERE draft_id = %s AND parse_status = 'parsed'
            """, (draft_id,))
            left = cur.fetchone()['n']

        if left == 0:
            with db_cursor() as (cur, conn):
                cur.execute("UPDATE sales_batch_drafts SET status = 'completed' WHERE draft_id = %s", (draft_id,))
            flash('Batch complete!', 'success')
            return redirect(url_for('sales_tracker.batch_summary', draft_id=draft_id))

        return redirect(url_for('sales_tracker.batch_review', draft_id=draft_id))

    return render_template('sales_tracker/batch_item_confirm.html',
        draft=draft, item=item, parsed=parsed, companies=companies,
        existing_clients=existing_clients, users=users, current_user_id=current_user.id,
        dup_invoice=None,
        default_commission=round((parsed.get('computed_total') or 0) * 0.01, 2),
        iso_date=_to_iso_date(parsed.get('invoice_date')), remaining=remaining)


@sales_tracker_bp.route('/upload/batch/<draft_id>/preview/<item_id>')
@login_required
@require_role('contributor')
def batch_item_pdf(draft_id, item_id):
    import io
    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT bi.pdf_bytes FROM sales_batch_draft_items bi
            JOIN sales_batch_drafts bd ON bi.draft_id = bd.draft_id
            WHERE bi.item_id = %s AND bi.draft_id = %s AND bd.user_id = %s
        """, (item_id, draft_id, current_user.id))
        row = cur.fetchone()
    if not row or not row['pdf_bytes']:
        return 'PDF not found', 404
    return send_file(io.BytesIO(bytes(row['pdf_bytes'])), mimetype='application/pdf')


@sales_tracker_bp.route('/upload/batch/<draft_id>/summary')
@login_required
@require_role('contributor')
def batch_summary(draft_id):
    with db_cursor() as (cur, _):
        cur.execute("SELECT * FROM sales_batch_drafts WHERE draft_id = %s AND user_id = %s",
                    (draft_id, current_user.id))
        draft = cur.fetchone()
        cur.execute("""
            SELECT bi.*, si.invoice_number AS saved_invoice_number, si.total AS saved_total
            FROM sales_batch_draft_items bi
            LEFT JOIN sales_invoices si ON bi.sales_invoice_id = si.sales_invoice_id
            WHERE bi.draft_id = %s ORDER BY bi.position
        """, (draft_id,))
        items = cur.fetchall()
    return render_template('sales_tracker/batch_summary.html', draft=draft, items=items)


@sales_tracker_bp.route('/upload/batch/<draft_id>/discard', methods=['POST'])
@login_required
@require_role('contributor')
def batch_discard(draft_id):
    with db_cursor() as (cur, conn):
        cur.execute("DELETE FROM sales_batch_drafts WHERE draft_id = %s AND user_id = %s",
                    (draft_id, current_user.id))
    flash('Batch discarded.', 'info')
    return redirect(url_for('sales_tracker.upload'))
