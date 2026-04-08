import bcrypt
from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from ..auth_utils import require_role
from ..db import db_cursor

manage_bp = Blueprint('manage', __name__, url_prefix='/manage')


# ── Users ─────────────────────────────────────────────────────────────────────

@manage_bp.route('/users')
@login_required
@require_role('admin')
def users():
    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT u.*,
                   ARRAY_AGG(c.company_name) FILTER (WHERE c.company_name IS NOT NULL) AS companies,
                   ARRAY_AGG(uc.company_id)  FILTER (WHERE uc.company_id  IS NOT NULL) AS company_ids
            FROM dim_users u
            LEFT JOIN user_companies uc ON u.user_id = uc.user_id
            LEFT JOIN dim_companies  c  ON uc.company_id = c.company_id
            GROUP BY u.user_id ORDER BY u.username
        """)
        users = cur.fetchall()
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE")
        companies = cur.fetchall()
    return render_template('manage/users.html', users=users, companies=companies)


@manage_bp.route('/users/<int:uid>', methods=['POST'])
@login_required
@require_role('admin')
def edit_user(uid):
    action = request.form.get('action')
    with db_cursor() as (cur, conn):
        if action == 'update':
            full_name   = request.form.get('full_name', '').strip()
            email       = request.form.get('email', '').strip()
            phone       = request.form.get('phone', '').strip()
            telegram_id = request.form.get('telegram_id', '').strip()
            portal_role = request.form.get('portal_role', 'none')
            is_active   = 'is_active' in request.form
            cur.execute("""
                UPDATE dim_users SET full_name=%s, email=%s, phone=%s,
                    telegram_id=%s, portal_role=%s, is_active=%s
                WHERE user_id=%s
            """, (full_name or None, email or None, phone or None,
                  telegram_id or None, portal_role, is_active, uid))

            # Update company memberships
            new_companies = request.form.getlist('company_ids', type=int)
            cur.execute("DELETE FROM user_companies WHERE user_id=%s", (uid,))
            for cid in new_companies:
                cur.execute("INSERT INTO user_companies (user_id, company_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                            (uid, cid))
            flash('User updated.', 'success')

        elif action == 'set_password':
            password = request.form.get('password', '')
            confirm  = request.form.get('password_confirm', '')
            if len(password) < 8:
                flash('Password must be at least 8 characters.', 'error')
                return redirect(url_for('manage.users'))
            if password != confirm:
                flash('Passwords do not match.', 'error')
                return redirect(url_for('manage.users'))
            hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
            cur.execute("UPDATE dim_users SET portal_password_hash=%s WHERE user_id=%s",
                        (hashed, uid))
            flash(f'Password updated successfully.', 'success')

        elif action == 'deactivate':
            cur.execute("UPDATE dim_users SET is_active=FALSE WHERE user_id=%s", (uid,))
            flash('User deactivated.', 'warning')

    return redirect(url_for('manage.users'))


@manage_bp.route('/users/new', methods=['POST'])
@login_required
@require_role('admin')
def new_user():
    username  = request.form.get('username', '').strip()
    full_name = request.form.get('full_name', '').strip()
    email     = request.form.get('email', '').strip()
    phone     = request.form.get('phone', '').strip()
    role      = request.form.get('portal_role', 'none')
    password  = request.form.get('password', '')

    if not username or not email:
        flash('Username and email are required.', 'error')
        return redirect(url_for('manage.users'))

    if role != 'none' and len(password) < 8:
        flash('Password must be at least 8 characters for portal users.', 'error')
        return redirect(url_for('manage.users'))

    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode() if password else None

    with db_cursor() as (cur, conn):
        try:
            cur.execute("""
                INSERT INTO dim_users
                    (username, full_name, email, phone, portal_role, portal_password_hash)
                VALUES (%s,%s,%s,%s,%s,%s) RETURNING user_id
            """, (username, full_name or None, email, phone or None, role, hashed))
            new_id = cur.fetchone()['user_id']

            company_ids = request.form.getlist('company_ids', type=int)
            for cid in company_ids:
                cur.execute(
                    "INSERT INTO user_companies (user_id, company_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (new_id, cid))
            flash(f'User {username} created successfully.', 'success')
        except Exception as e:
            if 'unique' in str(e).lower():
                flash(f'A user with email {email} already exists.', 'error')
            else:
                flash(f'Error creating user: {str(e)}', 'error')

    return redirect(url_for('manage.users'))


# ── Cards ─────────────────────────────────────────────────────────────────────

def _cards_incomplete_count(cur):
    cur.execute("""
        SELECT COUNT(*) AS n FROM dim_cards
        WHERE is_active=TRUE AND (
            card_name='Unknown Card' OR card_brand='Unknown' OR
            card_name IS NULL OR card_brand IS NULL OR
            user_id IS NULL OR credit_limit=0 OR credit_limit IS NULL
        )
    """)
    return cur.fetchone()['n']


@manage_bp.route('/cards')
@login_required
@require_role('contributor')
def cards():
    f_card_id  = request.args.get('card_id', '').strip()
    f_name     = request.args.get('card_name', '').strip()
    f_brand    = request.args.get('card_brand', '').strip()
    f_holder   = request.args.get('cardholder', type=int)
    f_company  = request.args.get('company', type=int)
    f_active   = request.args.get('active', '')

    conditions = []
    params = []

    if current_user.portal_role != 'admin':
        conditions.append("d.user_id = %s"); params.append(current_user.id)
    if f_card_id:
        conditions.append("d.card_id ILIKE %s"); params.append(f'%{f_card_id}%')
    if f_name:
        conditions.append("d.card_name ILIKE %s"); params.append(f'%{f_name}%')
    if f_brand:
        conditions.append("d.card_brand ILIKE %s"); params.append(f'%{f_brand}%')
    if f_holder:
        conditions.append("d.user_id = %s"); params.append(f_holder)
    if f_company:
        conditions.append("d.company_id = %s"); params.append(f_company)
    if f_active == 'active':
        conditions.append("d.is_active = TRUE")
    elif f_active == 'inactive':
        conditions.append("d.is_active = FALSE")

    where = ('WHERE ' + ' AND '.join(conditions)) if conditions else ''

    with db_cursor() as (cur, _):
        cur.execute(f"""
            SELECT d.*, u.username AS cardholder_name, c.company_name
            FROM dim_cards d
            LEFT JOIN dim_users u     ON d.user_id    = u.user_id
            LEFT JOIN dim_companies c ON d.company_id = c.company_id
            {where}
            ORDER BY d.is_active DESC, c.company_name, d.card_id
        """, params)
        cards = cur.fetchall()
        cur.execute("SELECT user_id, username FROM dim_users WHERE is_active=TRUE ORDER BY username")
        users = cur.fetchall()
        cur.execute("SELECT company_id, company_name FROM dim_companies WHERE is_active=TRUE")
        companies = cur.fetchall()
        incomplete_count = _cards_incomplete_count(cur) if current_user.portal_role == 'admin' else 0

    return render_template('manage/cards.html', cards=cards, users=users,
                           companies=companies, incomplete_count=incomplete_count,
                           is_admin=current_user.portal_role == 'admin',
                           filters={'card_id':f_card_id,'card_name':f_name,
                                    'card_brand':f_brand,'cardholder':f_holder,
                                    'company':f_company,'active':f_active})


@manage_bp.route('/cards/<string:card_id>', methods=['POST'])
@login_required
@require_role('admin')
def edit_card(card_id):
    action = request.form.get('action')
    with db_cursor() as (cur, conn):
        if action == 'update':
            card_name     = request.form.get('card_name', '').strip()
            card_brand    = request.form.get('card_brand', '').strip()
            user_id       = request.form.get('user_id', type=int)
            company_id    = request.form.get('company_id', type=int)
            credit_limit  = request.form.get('credit_limit', type=float)
            cashback_rate = request.form.get('cashback_rate', type=float)
            is_active     = 'is_active' in request.form

            # Store as decimal (e.g. input 1.5 → stored 0.015)
            if cashback_rate and cashback_rate > 1:
                cashback_rate = cashback_rate / 100

            cur.execute("""
                UPDATE dim_cards SET card_name=%s, card_brand=%s, user_id=%s,
                    company_id=%s, credit_limit=%s, cashback_rate=%s, is_active=%s
                WHERE card_id=%s
            """, (card_name, card_brand, user_id, company_id,
                  credit_limit, cashback_rate, is_active, card_id))
            flash(f'Card {card_id} updated.', 'success')

        elif action == 'toggle':
            cur.execute("UPDATE dim_cards SET is_active = NOT is_active WHERE card_id=%s", (card_id,))
            flash(f'Card {card_id} status toggled.', 'info')

    return redirect(url_for('manage.cards'))


@manage_bp.route('/cards/new', methods=['POST'])
@login_required
@require_role('admin')
def new_card():
    card_id       = request.form.get('card_id', '').strip().zfill(4)
    card_name     = request.form.get('card_name', '').strip()
    card_brand    = request.form.get('card_brand', '').strip()
    user_id       = request.form.get('user_id', type=int)
    company_id    = request.form.get('company_id', type=int)
    credit_limit  = request.form.get('credit_limit', type=float)
    cashback_rate = request.form.get('cashback_rate', type=float)

    if cashback_rate and cashback_rate > 1:
        cashback_rate = cashback_rate / 100

    if not card_id or not company_id:
        flash('Card ID and company are required.', 'error')
        return redirect(url_for('manage.cards'))

    with db_cursor() as (cur, conn):
        cur.execute("""
            INSERT INTO dim_cards (card_id, card_name, card_brand, user_id, company_id,
                credit_limit, cashback_rate)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (card_id) DO NOTHING
        """, (card_id, card_name, card_brand, user_id, company_id, credit_limit, cashback_rate))

    flash(f'Card {card_id} added.', 'success')
    return redirect(url_for('manage.cards'))


# ── Companies ─────────────────────────────────────────────────────────────────

@manage_bp.route('/companies')
@login_required
@require_role('admin')
def companies():
    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT c.*, COUNT(DISTINCT uc.user_id) AS user_count,
                   COUNT(DISTINCT d.card_id) AS card_count
            FROM dim_companies c
            LEFT JOIN user_companies uc ON c.company_id = uc.company_id
            LEFT JOIN dim_cards d        ON c.company_id = d.company_id
            GROUP BY c.company_id ORDER BY c.company_name
        """)
        companies = cur.fetchall()
    return render_template('manage/companies.html', companies=companies)


@manage_bp.route('/companies/<int:cid>', methods=['POST'])
@login_required
@require_role('admin')
def edit_company(cid):
    action = request.form.get('action')
    with db_cursor() as (cur, conn):
        if action == 'update':
            name       = request.form.get('company_name', '').strip()
            code       = request.form.get('company_short_code', '').strip().upper()
            is_active  = 'is_active' in request.form
            cur.execute("""
                UPDATE dim_companies SET company_name=%s, company_short_code=%s, is_active=%s
                WHERE company_id=%s
            """, (name, code, is_active, cid))
            flash('Company updated.', 'success')

    return redirect(url_for('manage.companies'))


@manage_bp.route('/companies/new', methods=['POST'])
@login_required
@require_role('admin')
def new_company():
    name = request.form.get('company_name', '').strip()
    code = request.form.get('company_short_code', '').strip().upper()
    if not name or not code:
        flash('Name and short code are required.', 'error')
        return redirect(url_for('manage.companies'))
    with db_cursor() as (cur, conn):
        cur.execute("INSERT INTO dim_companies (company_name, company_short_code) VALUES (%s,%s)",
                    (name, code))
    flash(f'Company {name} added.', 'success')
    return redirect(url_for('manage.companies'))
