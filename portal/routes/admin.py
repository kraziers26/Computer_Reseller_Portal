from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required
from ..auth_utils import require_role
from ..db import db_cursor

admin_bp = Blueprint('admin', __name__)


@admin_bp.route('/dashboard')
@login_required
@require_role('admin')
def dashboard():
    # Filters
    f_month     = request.args.get('month', '')
    f_year      = request.args.get('year', '')
    f_company   = request.args.get('company', type=int)
    f_retailer  = request.args.get('retailer', '')
    f_submitter = request.args.get('submitter', type=int)
    f_person    = request.args.get('person_by', type=int)
    f_card      = request.args.get('card', '')

    conditions = ["t.price_total > 0", "t.is_duplicate = FALSE", "t.is_active = TRUE"]
    params = []
    if f_month:
        conditions.append("TO_CHAR(t.purchase_date,'MM') = %s"); params.append(f_month)
    if f_year:
        conditions.append("TO_CHAR(t.purchase_date,'YYYY') = %s"); params.append(f_year)
    if f_company:
        conditions.append("t.company_id = %s"); params.append(f_company)
    if f_retailer:
        conditions.append("t.retailer = %s"); params.append(f_retailer)
    if f_submitter:
        conditions.append("t.submitted_by_user_id = %s"); params.append(f_submitter)
    if f_person:
        conditions.append("t.user_id = %s"); params.append(f_person)
    if f_card:
        conditions.append("t.card_id = %s"); params.append(f_card)

    where = 'WHERE ' + ' AND '.join(conditions)

    with db_cursor() as (cur, _):
        cur.execute(f"""
            SELECT
                COUNT(*)                                              AS total_orders,
                ROUND(SUM(t.price_total)::numeric, 2)                AS total_gmv,
                ROUND(SUM(COALESCE(t.gross_paid_amount,0))::numeric,2) AS total_gross_paid,
                ROUND(SUM(COALESCE(t.net_paid_amount,0))::numeric,2)   AS total_net_paid,
                ROUND(SUM(COALESCE(t.sales_payroll_tax_withheld,0))::numeric,2) AS total_tax,
                ROUND(SUM(COALESCE(t.cashback_value,0))::numeric,2)    AS total_cashback,
                COUNT(*) FILTER (WHERE t.review_status='Pending')      AS pending_count,
                COUNT(*) FILTER (WHERE t.is_duplicate=TRUE)            AS dup_count
            FROM transactions t {where}
        """, params)
        metrics = cur.fetchone()

        # Recent submissions
        cur.execute(f"""
            SELECT t.order_number, t.retailer, t.purchase_date,
                   t.price_total, t.review_status, t.submitted_at,
                   sub.username AS submitter_name,
                   per.username AS person_name,
                   c.company_name, t.card_id,
                   d.cashback_rate
            FROM transactions t
            LEFT JOIN dim_users sub    ON t.submitted_by_email = sub.email
            LEFT JOIN dim_users per    ON t.user_id    = per.user_id
            LEFT JOIN dim_companies c  ON t.company_id = c.company_id
            LEFT JOIN dim_cards d      ON t.card_id    = d.card_id
            {where}
            ORDER BY t.submitted_at DESC LIMIT 10
        """, params)
        recent = cur.fetchall()

        cur.execute("SELECT COUNT(*) AS n FROM v_pending_review")
        pending_total = cur.fetchone()['n']

        # Filter options
        cur.execute("SELECT DISTINCT retailer FROM transactions WHERE is_active=TRUE ORDER BY retailer")
        retailers = [r['retailer'] for r in cur.fetchall()]
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE ORDER BY company_name")
        companies = cur.fetchall()
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active=TRUE ORDER BY username")
        users = cur.fetchall()
        cur.execute("""
            SELECT d.card_id, d.cashback_rate FROM dim_cards d
            WHERE d.card_id IN (SELECT DISTINCT card_id FROM transactions WHERE card_id IS NOT NULL AND is_active=TRUE)
            ORDER BY d.card_id
        """)
        cards = cur.fetchall()

        # Years available
        cur.execute("SELECT DISTINCT TO_CHAR(purchase_date,'YYYY') AS yr FROM transactions WHERE purchase_date IS NOT NULL ORDER BY yr DESC")
        years = [r['yr'] for r in cur.fetchall()]

    return render_template('dashboard.html',
                           metrics=metrics, recent=recent, pending_total=pending_total,
                           retailers=retailers, companies=companies, users=users, cards=cards, years=years,
                           filters={'month':f_month,'year':f_year,'company':f_company,
                                    'retailer':f_retailer,'submitter':f_submitter,
                                    'person_by':f_person,'card':f_card})


@admin_bp.route('/submissions/all')
@login_required
@require_role('admin')
def all_submissions():
    page     = request.args.get('page', 1, type=int)
    per_page = 25
    offset   = (page - 1) * per_page

    f_retailer   = request.args.get('retailer', '')
    f_company    = request.args.get('company', type=int)
    f_status     = request.args.get('status', '')
    f_month      = request.args.get('month', '')
    f_duplicates = request.args.get('duplicates', '')
    f_submitter  = request.args.get('submitter', type=int)
    f_card       = request.args.get('card', '')
    f_person     = request.args.get('person_by', type=int)

    conditions = ["t.is_active = TRUE"]
    params = []
    if f_retailer:
        conditions.append("t.retailer = %s"); params.append(f_retailer)
    if f_company:
        conditions.append("t.company_id = %s"); params.append(f_company)
    if f_status:
        conditions.append("t.review_status = %s"); params.append(f_status)
    if f_month:
        conditions.append("t.purchase_year_month = %s"); params.append(f_month)
    if f_duplicates:
        conditions.append("t.is_duplicate = TRUE")
    if f_submitter:
        conditions.append("sub.user_id = %s"); params.append(f_submitter)
    if f_card:
        conditions.append("t.card_id = %s"); params.append(f_card)
    if f_person:
        conditions.append("t.user_id = %s"); params.append(f_person)

    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''

    with db_cursor() as (cur, _):
        cur.execute(f"""
            SELECT t.transaction_id, t.order_number, t.retailer,
                   t.purchase_date, t.price_total, t.order_type,
                   t.review_status, t.submitted_at, t.is_duplicate,
                   t.card_id, d.cashback_rate,
                   ROUND(COALESCE(t.gross_paid_amount,0)::numeric,2) AS gross_paid,
                   ROUND(COALESCE(t.net_paid_amount,0)::numeric,2)   AS net_paid,
                   ROUND(COALESCE(t.sales_payroll_tax_withheld,0)::numeric,2) AS tax_withheld,
                   ROUND(COALESCE(t.cashback_value,0)::numeric,2)    AS cashback,
                   sub.username AS submitter_name,
                   per.username AS person_name,
                   c.company_name, t.notes
            FROM transactions t
            LEFT JOIN dim_users sub    ON t.submitted_by_email = sub.email
            LEFT JOIN dim_users per    ON t.user_id    = per.user_id
            LEFT JOIN dim_companies c  ON t.company_id = c.company_id
            LEFT JOIN dim_cards d      ON t.card_id    = d.card_id
            {where}
            ORDER BY t.submitted_at DESC
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])
        submissions = cur.fetchall()

        cur.execute(f"""
            SELECT COUNT(*) AS n FROM transactions t
            LEFT JOIN dim_users sub ON t.submitted_by_email = sub.email
            {where}
        """, params)
        total = cur.fetchone()['n']

        cur.execute("SELECT DISTINCT retailer FROM transactions WHERE is_active=TRUE ORDER BY retailer")
        retailers = [r['retailer'] for r in cur.fetchall()]
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE")
        companies = cur.fetchall()
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active=TRUE ORDER BY username")
        users = cur.fetchall()
        cur.execute("""
            SELECT d.card_id, d.cashback_rate FROM dim_cards d
            WHERE d.card_id IN (SELECT DISTINCT card_id FROM transactions WHERE card_id IS NOT NULL AND is_active=TRUE)
            ORDER BY d.card_id
        """)
        cards = cur.fetchall()
        cur.execute("SELECT DISTINCT purchase_year_month FROM transactions WHERE is_active=TRUE ORDER BY purchase_year_month DESC")
        months = [r['purchase_year_month'] for r in cur.fetchall()]

    return render_template('all_submissions.html',
                           submissions=submissions, total=total,
                           page=page, per_page=per_page,
                           retailers=retailers, companies=companies, users=users, cards=cards, months=months,
                           filters={'retailer':f_retailer,'company':f_company,'status':f_status,
                                    'month':f_month,'duplicates':f_duplicates,'submitter':f_submitter,
                                    'card':f_card,'person_by':f_person})


@admin_bp.route('/submissions/<uuid:tid>', methods=['GET', 'POST'])
@login_required
@require_role('admin')
def review_submission(tid):
    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT t.*, sub.username AS submitter_name, per.username AS person_name,
                   c.company_name
            FROM transactions t
            LEFT JOIN dim_users sub    ON t.submitted_by_email = sub.email
            LEFT JOIN dim_users per    ON t.user_id    = per.user_id
            LEFT JOIN dim_companies c  ON t.company_id = c.company_id
            WHERE t.transaction_id = %s
        """, (str(tid),))
        txn = cur.fetchone()
        if not txn:
            flash('Transaction not found.', 'error')
            return redirect(url_for('admin.all_submissions'))
        cur.execute("SELECT * FROM transaction_items WHERE transaction_id=%s", (str(tid),))
        items = cur.fetchall()
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE")
        companies = cur.fetchall()
        cur.execute("SELECT card_id, card_name, card_brand, cashback_rate FROM dim_cards WHERE is_active=TRUE ORDER BY card_id")
        cards = cur.fetchall()
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active=TRUE ORDER BY username")
        users = cur.fetchall()

    if request.method == 'POST':
        action = request.form.get('action')
        with db_cursor() as (cur, conn):
            if action == 'approve':
                cur.execute("UPDATE transactions SET review_status='Reviewed', review_date=NOW() WHERE transaction_id=%s", (str(tid),))
                flash('Transaction approved.', 'success')
            elif action == 'flag':
                cur.execute("UPDATE transactions SET review_status='Flagged' WHERE transaction_id=%s", (str(tid),))
                flash('Transaction flagged.', 'warning')
            elif action == 'mark_duplicate':
                cur.execute("UPDATE transactions SET is_duplicate=TRUE, review_status='Flagged' WHERE transaction_id=%s", (str(tid),))
                flash('Marked as duplicate.', 'warning')
            elif action == 'inactivate':
                cur.execute("UPDATE transactions SET is_active=FALSE WHERE transaction_id=%s", (str(tid),))
                flash('Transaction inactivated. It will no longer appear in reports.', 'warning')
                return redirect(url_for('admin.all_submissions'))
            elif action == 'delete':
                cur.execute("DELETE FROM transaction_items WHERE transaction_id=%s", (str(tid),))
                cur.execute("DELETE FROM transactions WHERE transaction_id=%s", (str(tid),))
                flash('Transaction permanently deleted.', 'danger')
                return redirect(url_for('admin.all_submissions'))
            elif action == 'edit':
                card_id    = request.form.get('card_id') or None
                company_id = request.form.get('company_id', type=int)
                user_id    = request.form.get('user_id', type=int)
                order_type = request.form.get('order_type')
                price      = request.form.get('price_total', type=float)
                notes      = request.form.get('notes', '').strip()[:140] or None

                cashback_rate = cashback_value = None
                if card_id and price:
                    cur.execute("SELECT cashback_rate FROM dim_cards WHERE card_id=%s", (card_id,))
                    row = cur.fetchone()
                    if row:
                        cashback_rate  = float(row['cashback_rate'])
                        cashback_value = round(price * cashback_rate, 2)

                gross_paid   = round(price * 0.01, 2) if price else None
                net_paid     = round(gross_paid * 0.8, 2) if gross_paid else None
                tax_withheld = round(gross_paid * 0.2, 2) if gross_paid else None
                gross_biz    = round((gross_paid or 0)+(cashback_value or 0),2) if gross_paid else None
                net_biz      = round((gross_biz or 0)-(net_paid or 0),2) if gross_biz else None

                cur.execute("""
                    UPDATE transactions SET
                        card_id=%s, company_id=%s, user_id=%s, order_type=%s, price_total=%s,
                        cashback_rate=%s, cashback_value=%s,
                        gross_paid_amount=%s, net_paid_amount=%s,
                        gross_business_commission=%s, net_business_commission=%s,
                        sales_payroll_tax_withheld=%s, notes=%s,
                        review_status='Reviewed', review_date=NOW()
                    WHERE transaction_id=%s
                """, (card_id, company_id, user_id, order_type, price,
                      cashback_rate, cashback_value,
                      gross_paid, net_paid, gross_biz, net_biz, tax_withheld, notes,
                      str(tid)))
                flash('Transaction updated.', 'success')
        return redirect(url_for('admin.review_submission', tid=tid))

    return render_template('review_submission.html',
                           txn=txn, items=items, companies=companies, cards=cards, users=users)


@admin_bp.route('/payroll')
@login_required
@require_role('admin')
def payroll():
    month   = request.args.get('month', '')
    company = request.args.get('company', type=int)
    sort_by = request.args.get('sort', 'username')
    sort_dir = request.args.get('dir', 'asc')

    valid_sorts = {'username','order_count','total_purchases','gross_paid','net_paid','tax_withheld'}
    if sort_by not in valid_sorts:
        sort_by = 'username'
    order_clause = f"{sort_by} {'DESC' if sort_dir=='desc' else 'ASC'}"

    conditions = ["t.review_status != 'Flagged'", "t.is_duplicate = FALSE",
                  "t.price_total > 0", "t.is_active = TRUE"]
    params = []
    if month:
        conditions.append("t.purchase_year_month = %s"); params.append(month)

    where = 'WHERE ' + ' AND '.join(conditions)

    with db_cursor() as (cur, _):
        # Separate queries per company
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE ORDER BY company_name")
        companies = cur.fetchall()

        company_data = {}
        for comp in companies:
            if company and comp['company_id'] != company:
                continue
            cparams = params + [comp['company_id']]
            cur.execute(f"""
                SELECT
                    u.user_id, u.username,
                    t.purchase_year_month,
                    COUNT(t.transaction_id)                                    AS order_count,
                    ROUND(SUM(t.price_total)::numeric, 2)                      AS total_purchases,
                    ROUND(SUM(COALESCE(t.gross_paid_amount,0))::numeric,2)     AS gross_paid,
                    ROUND(SUM(COALESCE(t.net_paid_amount,0))::numeric,2)       AS net_paid,
                    ROUND(SUM(COALESCE(t.sales_payroll_tax_withheld,0))::numeric,2) AS tax_withheld
                FROM transactions t
                LEFT JOIN dim_users u ON t.user_id = u.user_id
                {where} AND t.company_id = %s
                GROUP BY u.user_id, u.username, t.purchase_year_month
                ORDER BY t.purchase_year_month DESC, {order_clause}
            """, cparams)
            company_data[comp['company_name']] = cur.fetchall()

        cur.execute("""
            SELECT DISTINCT purchase_year_month FROM transactions
            WHERE price_total > 0 AND is_active=TRUE ORDER BY purchase_year_month DESC
        """)
        months = [r['purchase_year_month'] for r in cur.fetchall()]

    return render_template('payroll.html',
                           company_data=company_data, months=months, companies=companies,
                           selected_month=month, selected_company=company,
                           sort_by=sort_by, sort_dir=sort_dir)


@admin_bp.route('/cashback')
@login_required
@require_role('admin')
def cashback():
    f_month   = request.args.get('month', '')
    f_year    = request.args.get('year', '')
    f_company = request.args.get('company', type=int)
    f_person  = request.args.get('person_by', type=int)

    conditions = ["t.is_active = TRUE"]
    t_params = []
    if f_month:
        conditions.append("TO_CHAR(t.purchase_date,'MM') = %s"); t_params.append(f_month)
    if f_year:
        conditions.append("TO_CHAR(t.purchase_date,'YYYY') = %s"); t_params.append(f_year)
    if f_person:
        conditions.append("t.user_id = %s"); t_params.append(f_person)
    if f_company:
        conditions.append("t.company_id = %s"); t_params.append(f_company)

    t_where = ('AND ' + ' AND '.join(conditions)) if conditions else ''

    with db_cursor() as (cur, _):
        cur.execute(f"""
            SELECT
                d.card_id, d.card_name, d.card_brand,
                u.username AS cardholder,
                c.company_name,
                d.cashback_rate,
                COUNT(t.transaction_id)                               AS transactions,
                ROUND(SUM(COALESCE(t.price_total,0))::numeric, 2)    AS total_spend,
                ROUND(SUM(COALESCE(t.cashback_value,0))::numeric, 2) AS total_cashback
            FROM dim_cards d
            LEFT JOIN transactions t  ON t.card_id    = d.card_id
                                      AND t.price_total > 0
                                      AND t.is_duplicate = FALSE
                                      {t_where}
            LEFT JOIN dim_users u     ON d.user_id    = u.user_id
            LEFT JOIN dim_companies c ON d.company_id = c.company_id
            WHERE d.is_active = TRUE
            GROUP BY d.card_id, d.card_name, d.card_brand,
                     u.username, c.company_name, d.cashback_rate
            ORDER BY total_cashback DESC NULLS LAST
        """, t_params)
        cashback_data = cur.fetchall()

        # Total cashback by company
        cur.execute(f"""
            SELECT c.company_name,
                   ROUND(SUM(COALESCE(t.cashback_value,0))::numeric,2) AS total
            FROM transactions t
            JOIN dim_companies c ON t.company_id = c.company_id
            WHERE t.price_total > 0 AND t.is_duplicate = FALSE {t_where}
            GROUP BY c.company_name ORDER BY total DESC
        """, t_params)
        company_cashback = cur.fetchall()

        cur.execute("SELECT DISTINCT TO_CHAR(purchase_date,'YYYY') AS yr FROM transactions WHERE purchase_date IS NOT NULL ORDER BY yr DESC")
        years = [r['yr'] for r in cur.fetchall()]
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE ORDER BY company_name")
        companies = cur.fetchall()
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active=TRUE ORDER BY username")
        users = cur.fetchall()

    return render_template('cashback.html',
                           cashback_data=cashback_data, company_cashback=company_cashback,
                           years=years, companies=companies, users=users,
                           filters={'month':f_month,'year':f_year,'company':f_company,'person_by':f_person})


@admin_bp.route('/print-batch', methods=['GET', 'POST'])
@login_required
@require_role('admin')
def print_batch():
    if request.method == 'POST':
        txn_ids  = request.form.getlist('txn_ids')
        batch_id = request.form.get('batch_id', '').strip()
        if txn_ids and batch_id:
            with db_cursor() as (cur, conn):
                cur.execute("""
                    UPDATE transactions SET print_batch_id=%s, print_date=NOW()
                    WHERE transaction_id = ANY(%s::uuid[])
                """, (batch_id, txn_ids))
            flash(f'{len(txn_ids)} invoices tagged as batch {batch_id}.', 'success')

    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT t.transaction_id, t.order_number, t.retailer,
                   t.purchase_date, t.price_total, t.print_date,
                   t.print_batch_id, t.invoice_file_path,
                   (t.invoice_pdf IS NOT NULL) AS has_pdf,
                   u.username, c.company_name
            FROM transactions t
            LEFT JOIN dim_users u     ON t.user_id    = u.user_id
            LEFT JOIN dim_companies c ON t.company_id = c.company_id
            WHERE t.print_date IS NULL AND t.review_status != 'Flagged' AND t.is_active=TRUE
            ORDER BY t.submitted_at DESC
        """)
        unprinted = cur.fetchall()
        cur.execute("""
            SELECT DISTINCT print_batch_id, MIN(print_date) AS batch_date, COUNT(*) AS count
            FROM transactions WHERE print_batch_id IS NOT NULL
            GROUP BY print_batch_id ORDER BY batch_date DESC LIMIT 20
        """)
        batches = cur.fetchall()

    return render_template('print_batch.html', unprinted=unprinted, batches=batches)


@admin_bp.route('/batch/<batch_id>')
@login_required
@require_role('admin')
def batch_detail(batch_id):
    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT t.transaction_id, t.order_number, t.retailer,
                   t.purchase_date, t.price_total, t.invoice_file_path,
                   (t.invoice_pdf IS NOT NULL) AS has_pdf,
                   t.print_date, u.username AS person_name, c.company_name
            FROM transactions t
            LEFT JOIN dim_users u     ON t.user_id    = u.user_id
            LEFT JOIN dim_companies c ON t.company_id = c.company_id
            WHERE t.print_batch_id = %s
            ORDER BY t.purchase_date
        """, (batch_id,))
        invoices = cur.fetchall()
        batch_date = invoices[0]['print_date'] if invoices else None
    return render_template('batch_detail.html', batch_id=batch_id,
                           invoices=invoices, batch_date=batch_date)


@admin_bp.route('/batch/unbatch', methods=['POST'])
@login_required
@require_role('admin')
def unbatch():
    batch_id = request.form.get('batch_id', '').strip()
    if batch_id:
        with db_cursor() as (cur, conn):
            cur.execute("""
                UPDATE transactions SET print_batch_id=NULL, print_date=NULL
                WHERE print_batch_id=%s
            """, (batch_id,))
        flash(f'Batch {batch_id} released. Invoices returned to unprinted queue.', 'success')
    return redirect(url_for('admin.print_batch'))


@admin_bp.route('/print-invoice/<string:tid>')
@login_required
@require_role('admin')
def print_invoice(tid):
    from flask import send_file, abort, redirect as redir
    import io, os
    with db_cursor() as (cur, _):
        cur.execute("SELECT invoice_file_path, invoice_pdf FROM transactions WHERE transaction_id=%s", (str(tid),))
        row = cur.fetchone()
    if not row:
        abort(404)
    # Priority 1: PDF stored in DB (survives redeploys)
    if row['invoice_pdf']:
        return send_file(io.BytesIO(bytes(row['invoice_pdf'])),
                         mimetype='application/pdf',
                         download_name=f'invoice-{tid[:8]}.pdf')
    # Priority 2: Google Drive / HTTP link
    path = row['invoice_file_path'] or ''
    if path.startswith('http'):
        return redir(path)
    # Priority 3: Local file still exists
    if path and os.path.exists(path):
        return send_file(path, mimetype='application/pdf')
    # Nothing available
    from flask import make_response
    return make_response("<h2>Invoice PDF unavailable</h2><p>This invoice was submitted before PDF storage was enabled. Re-upload the invoice to attach the PDF.</p><a href='javascript:history.back()'>← Go back</a>", 404)


@admin_bp.route('/payroll/export')
@login_required
@require_role('admin')
def export_payroll():
    import io
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment
    except ImportError:
        flash('openpyxl not installed. Add it to requirements.txt.', 'error')
        return redirect(url_for('admin.payroll'))
    from flask import send_file as sf

    month   = request.args.get('month', '')
    company = request.args.get('company', type=int)

    conditions = ["t.review_status != 'Flagged'", "t.is_duplicate=FALSE",
                  "t.price_total>0", "t.is_active=TRUE"]
    params = []
    if month:
        conditions.append("t.purchase_year_month=%s"); params.append(month)
    where = 'WHERE ' + ' AND '.join(conditions)

    with db_cursor() as (cur, _):
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE ORDER BY company_name")
        companies = cur.fetchall()

    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    header_font = Font(bold=True, color='FFFFFF')
    header_fill = PatternFill('solid', start_color='1a1d27')

    for comp in companies:
        if company and comp['company_id'] != company:
            continue
        with db_cursor() as (cur, _):
            cur.execute(f"""
                SELECT u.username, t.purchase_year_month,
                       COUNT(*) AS orders,
                       ROUND(SUM(t.price_total)::numeric,2) AS purchases,
                       ROUND(SUM(COALESCE(t.gross_paid_amount,0))::numeric,2) AS gross_paid,
                       ROUND(SUM(COALESCE(t.net_paid_amount,0))::numeric,2) AS net_paid,
                       ROUND(SUM(COALESCE(t.sales_payroll_tax_withheld,0))::numeric,2) AS tax_withheld
                FROM transactions t
                LEFT JOIN dim_users u ON t.user_id=u.user_id
                {where} AND t.company_id=%s
                GROUP BY u.username, t.purchase_year_month
                ORDER BY t.purchase_year_month DESC, u.username
            """, params + [comp['company_id']])
            rows = cur.fetchall()

        if not rows:
            continue

        ws = wb.create_sheet(title=comp['company_name'][:31])
        headers = ['Person', 'Month', 'Orders', 'Total Purchases', 'Gross Paid (1%)', 'Net Paid (0.8%)', 'Tax Withheld (0.2%)']
        for col, h in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=h)
            cell.font = header_font
            cell.fill = header_fill
            cell.alignment = Alignment(horizontal='center')

        for row_idx, r in enumerate(rows, 2):
            ws.cell(row=row_idx, column=1, value=r['username'] or '—')
            ws.cell(row=row_idx, column=2, value=r['purchase_year_month'])
            ws.cell(row=row_idx, column=3, value=r['orders'])
            for col, key in enumerate(['purchases','gross_paid','net_paid','tax_withheld'], 4):
                cell = ws.cell(row=row_idx, column=col, value=float(r[key] or 0))
                cell.number_format = '$#,##0.00'

        # Totals row
        total_row = len(rows) + 2
        ws.cell(row=total_row, column=1, value='TOTAL').font = Font(bold=True)
        ws.cell(row=total_row, column=3, value=f'=SUM(C2:C{total_row-1})').font = Font(bold=True)
        for col in range(4, 8):
            col_letter = chr(64+col)
            ws.cell(row=total_row, column=col, value=f'=SUM({col_letter}2:{col_letter}{total_row-1})').font = Font(bold=True)
            ws.cell(row=total_row, column=col).number_format = '$#,##0.00'

        for col in ws.columns:
            ws.column_dimensions[col[0].column_letter].width = 18

    fname = f"payroll{'_'+month if month else ''}.xlsx"
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return sf(buf, as_attachment=True, download_name=fname,
              mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
