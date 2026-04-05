from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_required
from ..auth_utils import require_role
from ..db import db_cursor

admin_bp = Blueprint('admin', __name__)


@admin_bp.route('/dashboard')
@login_required
@require_role('admin')
def dashboard():
    with db_cursor() as (cur, _):
        # Summary metrics
        cur.execute("""
            SELECT
                COUNT(*)                                        AS total_orders,
                ROUND(SUM(price_total)::numeric, 2)            AS total_gmv,
                ROUND(SUM(COALESCE(gross_paid_amount,0))::numeric,2) AS total_paid,
                ROUND(SUM(COALESCE(cashback_value,0))::numeric,2)    AS total_cashback,
                COUNT(*) FILTER (WHERE review_status='Pending')      AS pending_count,
                COUNT(*) FILTER (WHERE is_duplicate=TRUE)            AS dup_count
            FROM transactions WHERE price_total > 0
        """)
        metrics = cur.fetchone()

        # Monthly GMV last 6 months
        cur.execute("""
            SELECT purchase_year_month,
                   ROUND(SUM(price_total)::numeric,2) AS gmv,
                   COUNT(*) AS orders
            FROM transactions
            WHERE price_total > 0
              AND purchase_year_month >= TO_CHAR(NOW() - INTERVAL '6 months','YYYY-MM')
            GROUP BY purchase_year_month
            ORDER BY purchase_year_month
        """)
        monthly = cur.fetchall()

        # Recent submissions
        cur.execute("""
            SELECT t.order_number, t.retailer, t.purchase_date,
                   t.price_total, t.review_status, t.submitted_at,
                   u.username, c.company_name
            FROM transactions t
            LEFT JOIN dim_users u     ON t.user_id    = u.user_id
            LEFT JOIN dim_companies c ON t.company_id = c.company_id
            ORDER BY t.submitted_at DESC LIMIT 10
        """)
        recent = cur.fetchall()

        # Pending review
        cur.execute("SELECT COUNT(*) AS n FROM v_pending_review")
        pending_total = cur.fetchone()['n']

    return render_template('dashboard.html',
                           metrics=metrics, monthly=monthly,
                           recent=recent, pending_total=pending_total)


@admin_bp.route('/submissions/all')
@login_required
@require_role('admin')
def all_submissions():
    page     = request.args.get('page', 1, type=int)
    per_page = 25
    offset   = (page - 1) * per_page

    # Filters
    retailer   = request.args.get('retailer', '')
    company    = request.args.get('company', type=int)
    status     = request.args.get('status', '')
    month      = request.args.get('month', '')
    duplicates = request.args.get('duplicates', '')

    conditions = []
    params     = []
    if retailer:
        conditions.append("t.retailer = %s"); params.append(retailer)
    if company:
        conditions.append("t.company_id = %s"); params.append(company)
    if status:
        conditions.append("t.review_status = %s"); params.append(status)
    if month:
        conditions.append("t.purchase_year_month = %s"); params.append(month)
    if duplicates:
        conditions.append("t.is_duplicate = TRUE")

    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''

    with db_cursor() as (cur, _):
        cur.execute(f"""
            SELECT t.transaction_id, t.order_number, t.retailer,
                   t.purchase_date, t.price_total, t.order_type,
                   t.review_status, t.submitted_at, t.is_duplicate,
                   t.card_id, t.gross_paid_amount,
                   u.username, c.company_name
            FROM transactions t
            LEFT JOIN dim_users u     ON t.user_id    = u.user_id
            LEFT JOIN dim_companies c ON t.company_id = c.company_id
            {where}
            ORDER BY t.submitted_at DESC
            LIMIT %s OFFSET %s
        """, params + [per_page, offset])
        submissions = cur.fetchall()

        cur.execute(f"""
            SELECT COUNT(*) AS n
            FROM transactions t
            LEFT JOIN dim_users u     ON t.user_id    = u.user_id
            LEFT JOIN dim_companies c ON t.company_id = c.company_id
            {where}
        """, params)
        total = cur.fetchone()['n']

        cur.execute("SELECT DISTINCT retailer FROM transactions ORDER BY retailer")
        retailers = [r['retailer'] for r in cur.fetchall()]

        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE")
        companies = cur.fetchall()

    return render_template('all_submissions.html',
                           submissions=submissions, total=total,
                           page=page, per_page=per_page,
                           retailers=retailers, companies=companies,
                           filters={'retailer':retailer,'company':company,
                                    'status':status,'month':month,'duplicates':duplicates})


@admin_bp.route('/submissions/<uuid:tid>', methods=['GET', 'POST'])
@login_required
@require_role('admin')
def review_submission(tid):
    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT t.*, u.username, c.company_name
            FROM transactions t
            LEFT JOIN dim_users u     ON t.user_id    = u.user_id
            LEFT JOIN dim_companies c ON t.company_id = c.company_id
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

        cur.execute("SELECT card_id, card_name, card_brand FROM dim_cards WHERE is_active=TRUE ORDER BY card_id")
        cards = cur.fetchall()

    if request.method == 'POST':
        action = request.form.get('action')
        with db_cursor() as (cur, conn):
            if action == 'approve':
                cur.execute("""
                    UPDATE transactions SET review_status='Reviewed', review_date=NOW()
                    WHERE transaction_id=%s
                """, (str(tid),))
                flash('Transaction approved.', 'success')

            elif action == 'flag':
                cur.execute("""
                    UPDATE transactions SET review_status='Flagged'
                    WHERE transaction_id=%s
                """, (str(tid),))
                flash('Transaction flagged for follow-up.', 'warning')

            elif action == 'mark_duplicate':
                cur.execute("""
                    UPDATE transactions SET is_duplicate=TRUE, review_status='Flagged'
                    WHERE transaction_id=%s
                """, (str(tid),))
                flash('Marked as duplicate.', 'warning')

            elif action == 'edit':
                card_id    = request.form.get('card_id') or None
                company_id = request.form.get('company_id', type=int)
                order_type = request.form.get('order_type')
                price      = request.form.get('price_total', type=float)

                cashback_rate = cashback_value = None
                if card_id and price:
                    cur.execute("SELECT cashback_rate FROM dim_cards WHERE card_id=%s", (card_id,))
                    row = cur.fetchone()
                    if row:
                        cashback_rate  = float(row['cashback_rate'])
                        cashback_value = round(price * cashback_rate, 4)

                gross_paid   = round(price * 0.01, 4) if price else None
                net_paid     = round(gross_paid * 0.8, 4) if gross_paid else None
                tax_withheld = round(gross_paid * 0.2, 4) if gross_paid else None
                gross_biz    = round((gross_paid or 0)+(cashback_value or 0),4) if gross_paid else None
                net_biz      = round((gross_biz or 0)-(net_paid or 0),4) if gross_biz else None

                cur.execute("""
                    UPDATE transactions SET
                        card_id=%s, company_id=%s, order_type=%s, price_total=%s,
                        cashback_rate=%s, cashback_value=%s,
                        gross_paid_amount=%s, net_paid_amount=%s,
                        gross_business_commission=%s, net_business_commission=%s,
                        sales_payroll_tax_withheld=%s,
                        review_status='Reviewed', review_date=NOW()
                    WHERE transaction_id=%s
                """, (card_id, company_id, order_type, price,
                      cashback_rate, cashback_value,
                      gross_paid, net_paid, gross_biz, net_biz, tax_withheld,
                      str(tid)))
                flash('Transaction updated.', 'success')

        return redirect(url_for('admin.review_submission', tid=tid))

    return render_template('review_submission.html',
                           txn=txn, items=items,
                           companies=companies, cards=cards)


@admin_bp.route('/payroll')
@login_required
@require_role('admin')
def payroll():
    month = request.args.get('month', '')
    company = request.args.get('company', type=int)

    conditions = ["t.review_status != 'Flagged'", "t.is_duplicate = FALSE", "t.price_total > 0"]
    params = []
    if month:
        conditions.append("t.purchase_year_month = %s"); params.append(month)
    if company:
        conditions.append("t.company_id = %s"); params.append(company)

    where = 'WHERE ' + ' AND '.join(conditions)

    with db_cursor() as (cur, _):
        cur.execute(f"""
            SELECT
                u.user_id, u.username,
                c.company_name,
                t.purchase_year_month,
                COUNT(t.transaction_id)                                AS order_count,
                ROUND(SUM(t.price_total)::numeric, 2)                  AS total_purchases,
                ROUND(SUM(COALESCE(t.gross_paid_amount,0))::numeric,2) AS gross_paid,
                ROUND(SUM(COALESCE(t.net_paid_amount,0))::numeric,2)   AS net_paid,
                ROUND(SUM(COALESCE(t.sales_payroll_tax_withheld,0))::numeric,2) AS tax_withheld
            FROM transactions t
            LEFT JOIN dim_users u     ON t.user_id    = u.user_id
            LEFT JOIN dim_companies c ON t.company_id = c.company_id
            {where}
            GROUP BY u.user_id, u.username, c.company_name, t.purchase_year_month
            ORDER BY t.purchase_year_month DESC, u.username
        """, params)
        payroll_data = cur.fetchall()

        cur.execute("""
            SELECT DISTINCT purchase_year_month FROM transactions
            WHERE price_total > 0 ORDER BY purchase_year_month DESC
        """)
        months = [r['purchase_year_month'] for r in cur.fetchall()]

        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE")
        companies = cur.fetchall()

    return render_template('payroll.html',
                           payroll_data=payroll_data,
                           months=months, companies=companies,
                           selected_month=month, selected_company=company)


@admin_bp.route('/cashback')
@login_required
@require_role('admin')
def cashback():
    month = request.args.get('month', '')

    with db_cursor() as (cur, _):
        params = []
        month_filter = ''
        if month:
            month_filter = 'AND t.purchase_year_month = %s'
            params.append(month)

        cur.execute(f"""
            SELECT
                d.card_id, d.card_name, d.card_brand,
                u.username AS cardholder,
                c.company_name,
                d.cashback_rate,
                COUNT(t.transaction_id)                             AS transactions,
                ROUND(SUM(t.price_total)::numeric, 2)               AS total_spend,
                ROUND(SUM(COALESCE(t.cashback_value,0))::numeric,4) AS total_cashback
            FROM dim_cards d
            LEFT JOIN transactions t  ON t.card_id    = d.card_id
                                      AND t.price_total > 0
                                      AND t.is_duplicate = FALSE
                                      {month_filter}
            LEFT JOIN dim_users u     ON d.user_id    = u.user_id
            LEFT JOIN dim_companies c ON d.company_id = c.company_id
            WHERE d.is_active = TRUE
            GROUP BY d.card_id, d.card_name, d.card_brand,
                     u.username, c.company_name, d.cashback_rate
            ORDER BY total_cashback DESC NULLS LAST
        """, params)
        cashback_data = cur.fetchall()

        cur.execute("""
            SELECT DISTINCT purchase_year_month FROM transactions
            WHERE price_total > 0 ORDER BY purchase_year_month DESC
        """)
        months = [r['purchase_year_month'] for r in cur.fetchall()]

    return render_template('cashback.html',
                           cashback_data=cashback_data,
                           months=months, selected_month=month)


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
                    UPDATE transactions
                    SET print_batch_id = %s, print_date = NOW()
                    WHERE transaction_id = ANY(%s::uuid[])
                """, (batch_id, txn_ids))
            flash(f'{len(txn_ids)} invoices tagged as batch {batch_id}.', 'success')

    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT t.transaction_id, t.order_number, t.retailer,
                   t.purchase_date, t.price_total, t.print_date,
                   t.print_batch_id, u.username, c.company_name
            FROM transactions t
            LEFT JOIN dim_users u     ON t.user_id    = u.user_id
            LEFT JOIN dim_companies c ON t.company_id = c.company_id
            WHERE t.print_date IS NULL AND t.review_status != 'Flagged'
            ORDER BY t.purchase_date DESC
        """)
        unprinted = cur.fetchall()

        cur.execute("""
            SELECT DISTINCT print_batch_id, MIN(print_date) AS batch_date, COUNT(*) AS count
            FROM transactions
            WHERE print_batch_id IS NOT NULL
            GROUP BY print_batch_id
            ORDER BY batch_date DESC LIMIT 20
        """)
        batches = cur.fetchall()

    return render_template('print_batch.html', unprinted=unprinted, batches=batches)
