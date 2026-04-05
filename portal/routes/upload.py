import os, sys, uuid, json
from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from flask_login import login_required, current_user
from ..auth_utils import require_role
from ..db import db_cursor

sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'parsers'))

upload_bp = Blueprint('upload', __name__)

ALLOWED_EXT = {'pdf'}
UPLOAD_FOLDER = '/tmp/portal_uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXT


def run_parser(pdf_path):
    """Route PDF to correct parser based on content detection."""
    import costco_parser, bestbuy_parser, amazon_parser, apple_parser, walmart_parser

    parsers = [
        costco_parser.parse,
        bestbuy_parser.parse,
        amazon_parser.parse,
        apple_parser.parse,
        walmart_parser.parse,
    ]
    for parser_fn in parsers:
        try:
            result = parser_fn(pdf_path)
            if result:
                return result
        except Exception:
            continue
    return None


def invoice_to_dict(invoice):
    """Convert parsed invoice object to serializable dict."""
    items = []
    for item in getattr(invoice, 'items', []):
        items.append({
            'item_description': item.item_description,
            'sku_model_color':  item.sku_model_color,
            'quantity':         item.quantity,
            'unit_price':       float(item.unit_price),
            'line_total':       float(item.line_total),
        })
    return {
        'retailer':            invoice.retailer,
        'order_number':        invoice.order_number,
        'purchase_date':       str(invoice.purchase_date) if invoice.purchase_date else None,
        'purchase_year_month': invoice.purchase_year_month,
        'card_last4':          invoice.card_last4,
        'fulfillment_method':  getattr(invoice, 'fulfillment_method', None) or
                               getattr(invoice, 'order_type', 'Delivery'),
        'price_total':         float(invoice.price_total) if invoice.price_total else None,
        'costco_taxes_paid':   float(invoice.costco_taxes_paid)
                               if getattr(invoice, 'costco_taxes_paid', None) else None,
        'needs_review':        invoice.needs_review,
        'parse_errors':        invoice.parse_errors,
        'items':               items,
    }


@upload_bp.route('/upload', methods=['GET', 'POST'])
@login_required
@require_role('submitter')
def upload():
    with db_cursor() as (cur, _):
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE ORDER BY company_name")
        companies = cur.fetchall()

    if request.method == 'POST':
        if 'pdf' not in request.files:
            flash('No file selected.', 'error')
            return render_template('upload.html', companies=companies)

        f = request.files['pdf']
        if not f.filename or not allowed_file(f.filename):
            flash('Please upload a PDF file.', 'error')
            return render_template('upload.html', companies=companies)

        # Save temp file
        tmp_name = f'{uuid.uuid4()}.pdf'
        tmp_path = os.path.join(UPLOAD_FOLDER, tmp_name)
        f.save(tmp_path)

        # Parse
        invoice = run_parser(tmp_path)

        if not invoice:
            os.remove(tmp_path)
            flash('Could not read this PDF as a valid invoice. Please check the file and resubmit.', 'error')
            return render_template('upload.html', companies=companies)

        invoice_data = invoice_to_dict(invoice)
        invoice_data['_tmp_path'] = tmp_path

        # Store in session for confirmation step
        session['pending_invoice'] = json.dumps(invoice_data)
        return redirect(url_for('upload.confirm'))

    return render_template('upload.html', companies=companies)


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

    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'cancel':
            tmp = invoice_data.get('_tmp_path')
            if tmp and os.path.exists(tmp):
                os.remove(tmp)
            session.pop('pending_invoice', None)
            flash('Submission cancelled.', 'info')
            return redirect(url_for('upload.upload'))

        # Collect confirmed/corrected values from form
        company_id   = request.form.get('company_id', type=int)
        card_last4   = request.form.get('card_last4', '').strip().zfill(4) or None
        order_number = request.form.get('order_number', '').strip()
        price_total  = request.form.get('price_total', type=float)
        order_type   = request.form.get('order_type', 'Delivery')

        if not company_id:
            flash('Please select a company.', 'error')
            return render_template('confirm.html', invoice=invoice_data, companies=companies)

        # Lookup cashback rate
        cashback_rate = None
        cashback_value = None
        if card_last4:
            with db_cursor() as (cur, _):
                cur.execute("SELECT cashback_rate FROM dim_cards WHERE card_id=%s AND is_active=TRUE",
                            (card_last4,))
                row = cur.fetchone()
                if row:
                    cashback_rate  = float(row['cashback_rate'])
                    cashback_value = round(price_total * cashback_rate, 4) if price_total else None

        # Commission calculations
        gross_paid      = round(price_total * 0.01, 4) if price_total else None
        net_paid        = round(gross_paid * 0.8, 4) if gross_paid else None
        tax_withheld    = round(gross_paid * 0.2, 4) if gross_paid else None
        gross_biz       = round((gross_paid or 0) + (cashback_value or 0), 4) if gross_paid else None
        net_biz         = round((gross_biz or 0) - (net_paid or 0), 4) if gross_biz else None

        needs_review = invoice_data.get('needs_review', False) or not card_last4

        # Insert transaction
        tid = str(uuid.uuid4())
        with db_cursor() as (cur, conn):
            cur.execute("""
                INSERT INTO transactions (
                    transaction_id, order_number, retailer,
                    purchase_date, purchase_year_month,
                    user_id, company_id, card_id,
                    price_total, costco_taxes_paid,
                    cashback_rate, cashback_value,
                    commission_type, commission_amount, order_type,
                    invoice_file_path, review_status, is_duplicate, submitted_by_email,
                    gross_paid_amount, net_paid_amount,
                    gross_business_commission, net_business_commission,
                    sales_payroll_tax_withheld, submitted_at
                ) VALUES (
                    %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
                    %s,%s,'standard',%s,%s,%s,%s,FALSE,%s,
                    %s,%s,%s,%s,%s,NOW()
                )
            """, (
                tid,
                order_number,
                invoice_data['retailer'],
                invoice_data['purchase_date'],
                invoice_data['purchase_year_month'],
                current_user.id,
                company_id,
                card_last4,
                price_total,
                invoice_data.get('costco_taxes_paid'),
                cashback_rate, cashback_value,
                gross_paid, order_type,
                invoice_data.get('_tmp_path'),
                'Pending' if needs_review else 'Auto-approved',
                current_user.email,
                gross_paid, net_paid, gross_biz, net_biz, tax_withheld
            ))

            # Insert line items if present
            items = invoice_data.get('items', [])
            if items:
                item_rows = [(str(uuid.uuid4()), tid, it['item_description'],
                              it['sku_model_color'], it['quantity'],
                              it['unit_price'], it['line_total']) for it in items]
                cur.executemany("""
                    INSERT INTO transaction_items
                    (item_id, transaction_id, item_description, sku_model_color,
                     quantity, unit_price, line_total)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, item_rows)

        session.pop('pending_invoice', None)
        tmp = invoice_data.get('_tmp_path')
        if tmp and os.path.exists(tmp):
            os.remove(tmp)

        flash(f'Order #{order_number} submitted successfully!', 'success')
        return redirect(url_for('upload.my_submissions'))

    return render_template('confirm.html', invoice=invoice_data, companies=companies)


@upload_bp.route('/submissions/mine')
@login_required
@require_role('submitter')
def my_submissions():
    page = request.args.get('page', 1, type=int)
    per_page = 20
    offset = (page - 1) * per_page

    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT t.transaction_id, t.order_number, t.retailer,
                   t.purchase_date, t.price_total, t.order_type,
                   t.review_status, t.submitted_at, c.company_name,
                   t.card_id, t.is_duplicate
            FROM transactions t
            LEFT JOIN dim_companies c ON t.company_id = c.company_id
            WHERE t.user_id = %s
            ORDER BY t.submitted_at DESC
            LIMIT %s OFFSET %s
        """, (current_user.id, per_page, offset))
        submissions = cur.fetchall()

        cur.execute("SELECT COUNT(*) AS n FROM transactions WHERE user_id=%s",
                    (current_user.id,))
        total = cur.fetchone()['n']

    return render_template('my_submissions.html',
                           submissions=submissions,
                           page=page, per_page=per_page, total=total)
