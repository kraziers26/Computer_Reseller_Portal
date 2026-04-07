import os, sys, uuid, json
from flask import (Blueprint, render_template, request, redirect,
                   url_for, flash, session, send_file, jsonify)
from flask_login import login_required, current_user
from ..auth_utils import require_role
from ..db import db_cursor

sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
    'parsers'))

upload_bp = Blueprint('upload', __name__)
ALLOWED_EXT = {'pdf'}
UPLOAD_FOLDER = '/tmp/portal_uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


def run_parser(pdf_path):
    import costco_parser, bestbuy_parser, amazon_parser, apple_parser, walmart_parser
    for fn in [costco_parser.parse, bestbuy_parser.parse, amazon_parser.parse,
               apple_parser.parse, walmart_parser.parse]:
        try:
            r = fn(pdf_path)
            if r:
                return r
        except Exception:
            continue
    return None


def invoice_to_dict(invoice):
    raw = getattr(invoice, 'items', [])
    if callable(raw):
        raw = []
    items = []
    for item in (raw if isinstance(raw, list) else []):
        try:
            items.append({'item_description': item.item_description,
                          'sku_model_color': item.sku_model_color,
                          'quantity': item.quantity,
                          'unit_price': float(item.unit_price),
                          'line_total': float(item.line_total)})
        except Exception:
            continue
    return {'retailer': invoice.retailer,
            'order_number': invoice.order_number,
            'purchase_date': str(invoice.purchase_date) if invoice.purchase_date else None,
            'purchase_year_month': invoice.purchase_year_month,
            'card_last4': invoice.card_last4,
            'fulfillment_method': getattr(invoice, 'fulfillment_method', None) or 'Delivery',
            'price_total': float(invoice.price_total) if invoice.price_total else None,
            'costco_taxes_paid': float(invoice.costco_taxes_paid)
                                 if getattr(invoice, 'costco_taxes_paid', None) else None,
            'needs_review': invoice.needs_review,
            'parse_errors': invoice.parse_errors,
            'items': items}


def save_transaction(form, invoice_data, pdf_bytes, form_user_id, current_email):
    """Write one confirmed transaction to DB. Returns transaction_id."""
    company_id   = form.get('company_id', type=int)
    card_last4   = (form.get('card_last4', '').strip() or None)
    if card_last4:
        card_last4 = card_last4.zfill(4)
    order_number = form.get('order_number', '').strip()
    price_total  = form.get('price_total', type=float)
    order_type   = form.get('order_type', 'Delivery')
    notes        = (form.get('notes', '').strip())[:140] or None

    cashback_rate = cashback_value = None
    if card_last4:
        with db_cursor() as (cur, _):
            cur.execute("SELECT cashback_rate FROM dim_cards WHERE card_id=%s AND is_active=TRUE",
                        (card_last4,))
            row = cur.fetchone()
            if row:
                cashback_rate  = float(row['cashback_rate'])
                cashback_value = round(price_total * cashback_rate, 2) if price_total else None

    gross_paid   = round(price_total * 0.01, 2) if price_total else None
    net_paid     = round(gross_paid * 0.8, 2) if gross_paid else None
    tax_withheld = round(gross_paid * 0.2, 2) if gross_paid else None
    gross_biz    = round((gross_paid or 0)+(cashback_value or 0), 2) if gross_paid else None
    net_biz      = round((gross_biz or 0)-(net_paid or 0), 2) if gross_biz else None
    needs_review = invoice_data.get('needs_review', False) or not card_last4

    tid = str(uuid.uuid4())
    with db_cursor() as (cur, conn):
        cur.execute("""
            INSERT INTO transactions (
                transaction_id, order_number, retailer, purchase_date, purchase_year_month,
                user_id, company_id, card_id, price_total, costco_taxes_paid,
                cashback_rate, cashback_value, commission_type, commission_amount, order_type,
                review_status, is_duplicate, submitted_by_email,
                gross_paid_amount, net_paid_amount, gross_business_commission,
                net_business_commission, sales_payroll_tax_withheld, notes,
                invoice_pdf, submitted_at
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'standard',%s,%s,%s,FALSE,%s,
                      %s,%s,%s,%s,%s,%s,%s,NOW())
        """, (tid, order_number, invoice_data['retailer'],
              invoice_data['purchase_date'], invoice_data['purchase_year_month'],
              form_user_id, company_id, card_last4, price_total,
              invoice_data.get('costco_taxes_paid'), cashback_rate, cashback_value,
              gross_paid, order_type,
              'Pending' if needs_review else 'Auto-approved',
              current_email, gross_paid, net_paid, gross_biz, net_biz, tax_withheld,
              notes, pdf_bytes))

        items = invoice_data.get('items', [])
        if items:
            cur.executemany("""
                INSERT INTO transaction_items
                (item_id, transaction_id, item_description, sku_model_color,
                 quantity, unit_price, line_total)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, [(str(uuid.uuid4()), tid, it['item_description'],
                   it.get('sku_model_color'), it['quantity'],
                   it['unit_price'], it['line_total']) for it in items])
    return tid


# ── Single Upload ─────────────────────────────────────────────────────────────

@upload_bp.route('/upload', methods=['GET', 'POST'])
@login_required
@require_role('submitter')
def upload():
    # Check for active batch draft
    active_draft = None
    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT d.draft_id, d.total_files, d.completed_count, d.created_at
            FROM batch_drafts d
            WHERE d.user_id=%s AND d.status='active'
            ORDER BY d.created_at DESC LIMIT 1
        """, (current_user.id,))
        active_draft = cur.fetchone()
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE ORDER BY company_name")
        companies = cur.fetchall()

    if request.method == 'POST':
        if 'pdf' not in request.files:
            flash('No file selected.', 'error')
            return render_template('upload.html', companies=companies, active_draft=active_draft)
        f = request.files['pdf']
        if not f.filename or not allowed_file(f.filename):
            flash('Please upload a PDF file.', 'error')
            return render_template('upload.html', companies=companies, active_draft=active_draft)
        tmp_path = os.path.join(UPLOAD_FOLDER, f'{uuid.uuid4()}.pdf')
        f.save(tmp_path)
        invoice = run_parser(tmp_path)
        if not invoice:
            os.remove(tmp_path)
            flash('Could not read this PDF as a valid invoice. Try Manual Entry instead.', 'error')
            return render_template('upload.html', companies=companies, active_draft=active_draft)
        invoice_data = invoice_to_dict(invoice)
        invoice_data['_tmp_path'] = tmp_path
        session['pending_invoice'] = json.dumps(invoice_data)
        return redirect(url_for('upload.confirm'))

    return render_template('upload.html', companies=companies, active_draft=active_draft)


@upload_bp.route('/upload/preview-pdf')
@login_required
@require_role('submitter')
def serve_pdf():
    if 'pending_invoice' not in session:
        return 'No pending invoice', 404
    data = json.loads(session['pending_invoice'])
    tmp_path = data.get('_tmp_path', '')
    if tmp_path and os.path.exists(tmp_path):
        return send_file(tmp_path, mimetype='application/pdf')
    return 'PDF not found', 404


@upload_bp.route('/upload/confirm', methods=['GET', 'POST'])
@login_required
@require_role('submitter')
def confirm():
    if 'pending_invoice' not in session:
        return redirect(url_for('upload.upload'))
    invoice_data = json.loads(session['pending_invoice'])

    with db_cursor() as (cur, _):
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE ORDER BY company_name")
        companies = cur.fetchall()
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active=TRUE ORDER BY username")
        users = cur.fetchall()

    if request.method == 'POST':
        if request.form.get('action') == 'cancel':
            tmp = invoice_data.get('_tmp_path')
            if tmp and os.path.exists(tmp):
                os.remove(tmp)
            session.pop('pending_invoice', None)
            return redirect(url_for('upload.upload'))

        tmp_path = invoice_data.get('_tmp_path', '')
        pdf_bytes = None
        if tmp_path and os.path.exists(tmp_path):
            with open(tmp_path, 'rb') as pf:
                pdf_bytes = pf.read()

        tid = save_transaction(request.form, invoice_data, pdf_bytes,
                               request.form.get('user_id', type=int) or current_user.id,
                               current_user.email)

        session.pop('pending_invoice', None)
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)

        flash(f"Order #{request.form.get('order_number','')} submitted!", 'success')
        return redirect(url_for('upload.my_submissions'))

    return render_template('confirm.html', invoice=invoice_data, companies=companies,
                           users=users, current_user_id=current_user.id)


# ── Batch Upload ──────────────────────────────────────────────────────────────

@upload_bp.route('/upload/batch', methods=['GET', 'POST'])
@login_required
@require_role('submitter')
def batch_upload():
    # Check for existing active draft
    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT d.draft_id, d.total_files, d.completed_count, d.failed_count, d.created_at
            FROM batch_drafts d WHERE d.user_id=%s AND d.status='active'
            ORDER BY d.created_at DESC LIMIT 1
        """, (current_user.id,))
        existing_draft = cur.fetchone()

    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'discard_draft' and existing_draft:
            with db_cursor() as (cur, conn):
                cur.execute("UPDATE batch_drafts SET status='discarded' WHERE draft_id=%s",
                            (str(existing_draft['draft_id']),))
            flash('Previous draft discarded.', 'info')
            return redirect(url_for('upload.batch_upload'))

        if action == 'upload':
            files = request.files.getlist('pdfs')
            if not files or not any(f.filename for f in files):
                flash('Please select at least one PDF.', 'error')
                return redirect(url_for('upload.batch_upload'))

            # Create new draft
            draft_id = str(uuid.uuid4())
            valid_files = [f for f in files if f.filename and allowed_file(f.filename)]

            with db_cursor() as (cur, conn):
                cur.execute("""
                    INSERT INTO batch_drafts (draft_id, user_id, total_files)
                    VALUES (%s,%s,%s)
                """, (draft_id, current_user.id, len(valid_files)))

                for pos, f in enumerate(valid_files):
                    tmp_path = os.path.join(UPLOAD_FOLDER, f'{uuid.uuid4()}.pdf')
                    f.save(tmp_path)

                    # Parse
                    invoice = run_parser(tmp_path)
                    pdf_bytes = open(tmp_path, 'rb').read()
                    os.remove(tmp_path)

                    if invoice:
                        inv_dict = invoice_to_dict(invoice)
                        cur.execute("""
                            INSERT INTO batch_draft_items
                            (draft_id, position, filename, parse_status, invoice_data, pdf_bytes)
                            VALUES (%s,%s,%s,'parsed',%s,%s)
                        """, (draft_id, pos, f.filename, json.dumps(inv_dict), pdf_bytes))
                    else:
                        cur.execute("""
                            INSERT INTO batch_draft_items
                            (draft_id, position, filename, parse_status, error_message, pdf_bytes)
                            VALUES (%s,%s,%s,'failed','Could not parse this PDF',%s)
                        """, (draft_id, pos, f.filename, pdf_bytes))

            flash(f'Batch created with {len(valid_files)} files.', 'success')
            return redirect(url_for('upload.batch_review', draft_id=draft_id))

    return render_template('batch_upload.html', existing_draft=existing_draft)


@upload_bp.route('/upload/batch/<draft_id>')
@login_required
@require_role('submitter')
def batch_review(draft_id):
    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT * FROM batch_drafts WHERE draft_id=%s AND user_id=%s
        """, (draft_id, current_user.id))
        draft = cur.fetchone()
        if not draft:
            flash('Batch not found.', 'error')
            return redirect(url_for('upload.upload'))

        cur.execute("""
            SELECT * FROM batch_draft_items
            WHERE draft_id=%s ORDER BY position
        """, (draft_id,))
        items = cur.fetchall()

        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE ORDER BY company_name")
        companies = cur.fetchall()

    # Find next unreviewed parsed item
    next_item = next((i for i in items if i['parse_status'] == 'parsed'), None)

    return render_template('batch_review.html', draft=draft, items=items,
                           companies=companies, next_item=next_item)


@upload_bp.route('/upload/batch/<draft_id>/item/<item_id>', methods=['GET', 'POST'])
@login_required
@require_role('submitter')
def batch_item_confirm(draft_id, item_id):
    with db_cursor() as (cur, _):
        cur.execute("SELECT * FROM batch_drafts WHERE draft_id=%s AND user_id=%s",
                    (draft_id, current_user.id))
        draft = cur.fetchone()
        if not draft:
            flash('Batch not found.', 'error')
            return redirect(url_for('upload.upload'))

        cur.execute("SELECT * FROM batch_draft_items WHERE item_id=%s AND draft_id=%s",
                    (item_id, draft_id))
        item = cur.fetchone()
        if not item:
            flash('Item not found.', 'error')
            return redirect(url_for('upload.batch_review', draft_id=draft_id))

        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE ORDER BY company_name")
        companies = cur.fetchall()
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active=TRUE ORDER BY username")
        users = cur.fetchall()

        # Count remaining
        cur.execute("""
            SELECT COUNT(*) AS n FROM batch_draft_items
            WHERE draft_id=%s AND parse_status='parsed'
        """, (draft_id,))
        remaining = cur.fetchone()['n']

    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'skip':
            with db_cursor() as (cur, conn):
                cur.execute("UPDATE batch_draft_items SET parse_status='skipped' WHERE item_id=%s",
                            (item_id,))
            flash('Invoice skipped.', 'info')
            return redirect(url_for('upload.batch_review', draft_id=draft_id))

        if action == 'submit':
            invoice_data = item['invoice_data']
            if isinstance(invoice_data, str):
                invoice_data = json.loads(invoice_data)

            pdf_bytes = bytes(item['pdf_bytes']) if item['pdf_bytes'] else None
            form_user_id = request.form.get('user_id', type=int) or current_user.id

            tid = save_transaction(request.form, invoice_data, pdf_bytes,
                                   form_user_id, current_user.email)

            with db_cursor() as (cur, conn):
                cur.execute("""
                    UPDATE batch_draft_items
                    SET parse_status='submitted', transaction_id=%s, submitted_at=NOW()
                    WHERE item_id=%s
                """, (tid, item_id))
                cur.execute("""
                    UPDATE batch_drafts
                    SET completed_count=completed_count+1, updated_at=NOW()
                    WHERE draft_id=%s
                """, (draft_id,))

            flash(f"Order #{request.form.get('order_number','')} submitted!", 'success')

            # Check if batch complete
            with db_cursor() as (cur, _):
                cur.execute("""
                    SELECT COUNT(*) AS n FROM batch_draft_items
                    WHERE draft_id=%s AND parse_status='parsed'
                """, (draft_id,))
                left = cur.fetchone()['n']

            if left == 0:
                with db_cursor() as (cur, conn):
                    cur.execute("UPDATE batch_drafts SET status='completed' WHERE draft_id=%s",
                                (draft_id,))
                flash('Batch complete!', 'success')
                return redirect(url_for('upload.batch_summary', draft_id=draft_id))

            return redirect(url_for('upload.batch_review', draft_id=draft_id))

    invoice_data = item['invoice_data']
    if isinstance(invoice_data, str):
        invoice_data = json.loads(invoice_data)

    return render_template('batch_item_confirm.html',
                           draft=draft, item=item, invoice=invoice_data,
                           companies=companies, users=users,
                           current_user_id=current_user.id, remaining=remaining)


@upload_bp.route('/upload/batch/<draft_id>/preview/<item_id>')
@login_required
@require_role('submitter')
def batch_item_pdf(draft_id, item_id):
    import io
    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT bi.pdf_bytes FROM batch_draft_items bi
            JOIN batch_drafts bd ON bi.draft_id=bd.draft_id
            WHERE bi.item_id=%s AND bi.draft_id=%s AND bd.user_id=%s
        """, (item_id, draft_id, current_user.id))
        row = cur.fetchone()
    if not row or not row['pdf_bytes']:
        return 'PDF not found', 404
    return send_file(io.BytesIO(bytes(row['pdf_bytes'])), mimetype='application/pdf')


@upload_bp.route('/upload/batch/<draft_id>/summary')
@login_required
@require_role('submitter')
def batch_summary(draft_id):
    with db_cursor() as (cur, _):
        cur.execute("SELECT * FROM batch_drafts WHERE draft_id=%s AND user_id=%s",
                    (draft_id, current_user.id))
        draft = cur.fetchone()
        cur.execute("""
            SELECT bi.*, t.order_number AS txn_order_number
            FROM batch_draft_items bi
            LEFT JOIN transactions t ON bi.transaction_id=t.transaction_id::uuid
            WHERE bi.draft_id=%s ORDER BY bi.position
        """, (draft_id,))
        items = cur.fetchall()
    return render_template('batch_summary.html', draft=draft, items=items)


@upload_bp.route('/upload/batch/<draft_id>/discard', methods=['POST'])
@login_required
@require_role('submitter')
def batch_discard(draft_id):
    with db_cursor() as (cur, conn):
        cur.execute("""
            UPDATE batch_drafts SET status='discarded'
            WHERE draft_id=%s AND user_id=%s
        """, (draft_id, current_user.id))
    flash('Batch discarded.', 'info')
    return redirect(url_for('upload.upload'))


# ── Manual Entry ──────────────────────────────────────────────────────────────

@upload_bp.route('/upload/manual', methods=['GET', 'POST'])
@login_required
@require_role('submitter')
def manual_upload():
    with db_cursor() as (cur, _):
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE ORDER BY company_name")
        companies = cur.fetchall()
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active=TRUE ORDER BY username")
        users = cur.fetchall()
        cur.execute("SELECT card_id, card_name, cashback_rate FROM dim_cards WHERE is_active=TRUE ORDER BY card_id")
        cards = cur.fetchall()

    if request.method == 'POST':
        # Build invoice_data manually from form
        purchase_date = request.form.get('purchase_date', '')
        year_month    = purchase_date[:7] if purchase_date else ''
        invoice_data  = {
            'retailer':            request.form.get('retailer', 'Other'),
            'order_number':        request.form.get('order_number', '').strip(),
            'purchase_date':       purchase_date,
            'purchase_year_month': year_month,
            'card_last4':          None,
            'fulfillment_method':  request.form.get('order_type', 'Delivery'),
            'price_total':         request.form.get('price_total', type=float),
            'costco_taxes_paid':   request.form.get('costco_taxes', type=float) or None,
            'needs_review':        True,
            'parse_errors':        [],
            'items': [{
                'item_description': request.form.get('item_description', ''),
                'sku_model_color':  request.form.get('sku', ''),
                'quantity':         request.form.get('quantity', type=int) or 1,
                'unit_price':       request.form.get('unit_price', type=float) or 0,
                'line_total':       request.form.get('price_total', type=float) or 0,
            }] if request.form.get('item_description') else []
        }

        # Handle optional PDF upload
        pdf_bytes = None
        if 'pdf' in request.files:
            f = request.files['pdf']
            if f.filename and allowed_file(f.filename):
                pdf_bytes = f.read()

        form_user_id = request.form.get('user_id', type=int) or current_user.id
        tid = save_transaction(request.form, invoice_data, pdf_bytes,
                               form_user_id, current_user.email)

        flash(f"Order #{invoice_data['order_number']} manually submitted!", 'success')
        return redirect(url_for('upload.my_submissions'))

    return render_template('manual_upload.html', companies=companies,
                           users=users, cards=cards, current_user_id=current_user.id)


# ── My Submissions ────────────────────────────────────────────────────────────

@upload_bp.route('/submissions/mine')
@login_required
@require_role('submitter')
def my_submissions():
    page     = request.args.get('page', 1, type=int)
    per_page = 20
    offset   = (page - 1) * per_page

    f_retailer = request.args.get('retailer', '')
    f_company  = request.args.get('company', type=int)
    f_type     = request.args.get('order_type', '')
    f_status   = request.args.get('status', '')
    f_notes    = request.args.get('notes', '')
    f_order    = request.args.get('order_number', '')

    conditions = ["t.submitted_by_email = %s"]
    params = [current_user.email]
    if f_retailer: conditions.append("t.retailer=%s"); params.append(f_retailer)
    if f_company:  conditions.append("t.company_id=%s"); params.append(f_company)
    if f_type:     conditions.append("t.order_type=%s"); params.append(f_type)
    if f_status:   conditions.append("t.review_status=%s"); params.append(f_status)
    if f_notes:    conditions.append("t.notes ILIKE %s"); params.append(f'%{f_notes}%')
    if f_order:    conditions.append("t.order_number ILIKE %s"); params.append(f'%{f_order}%')

    where = 'WHERE ' + ' AND '.join(conditions)

    with db_cursor() as (cur, _):
        cur.execute(f"""
            SELECT t.transaction_id, t.order_number, t.retailer,
                   t.purchase_date, t.price_total, t.order_type,
                   t.review_status, t.submitted_at, c.company_name,
                   t.card_id, t.is_duplicate, u.username AS person_name, t.notes
            FROM transactions t
            LEFT JOIN dim_companies c ON t.company_id=c.company_id
            LEFT JOIN dim_users u     ON t.user_id=u.user_id
            {where}
            ORDER BY t.submitted_at DESC LIMIT %s OFFSET %s
        """, params + [per_page, offset])
        submissions = cur.fetchall()

        cur.execute(f"SELECT COUNT(*) AS n FROM transactions t {where}", params)
        total = cur.fetchone()['n']

        cur.execute("SELECT DISTINCT retailer FROM transactions WHERE submitted_by_email=%s ORDER BY retailer",
                    (current_user.email,))
        retailers = [r['retailer'] for r in cur.fetchall()]
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE")
        companies = cur.fetchall()

    return render_template('my_submissions.html', submissions=submissions,
                           page=page, per_page=per_page, total=total,
                           retailers=retailers, companies=companies,
                           filters={'retailer':f_retailer,'company':f_company,
                                    'order_type':f_type,'status':f_status,
                                    'notes':f_notes,'order_number':f_order})
