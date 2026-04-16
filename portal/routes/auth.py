from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_user, logout_user, login_required, current_user
from ..models import User

auth_bp = Blueprint('auth', __name__)

@auth_bp.route('/', methods=['GET'])
def index():
    if current_user.is_authenticated:
        if current_user.is_admin:
            return redirect(url_for('admin.dashboard'))
        return redirect(url_for('upload.upload'))
    return redirect(url_for('auth.login'))

@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    from ..security import check_login_lockout, record_failed_login, record_successful_login
    if current_user.is_authenticated:
        if current_user.portal_role == 'admin':
            return redirect(url_for('admin.dashboard'))
        return redirect(url_for('upload.upload'))

    if request.method == 'POST':
        email    = request.form.get('email', '').strip().lower()
        password = request.form.get('password', '')

        # Check lockout
        locked, mins_left = check_login_lockout(email)
        if locked:
            flash(f'Too many failed attempts. Try again in {mins_left} minute(s).', 'error')
            return render_template('login.html')

        user = User.get_by_email(email)
        if user and user.check_password(password):
            login_user(user)
            record_successful_login(email)
            if user.portal_role == 'admin':
                return redirect(url_for('admin.dashboard'))
            return redirect(url_for('upload.upload'))

        record_failed_login(email)
        locked, mins_left = check_login_lockout(email)
        if locked:
            flash(f'Too many failed attempts. Account locked for {mins_left} minute(s).', 'error')
        else:
            flash('Invalid email or password.', 'error')

    return render_template('login.html')

@auth_bp.route('/logout')
@login_required
def logout():
    from ..security import audit
    audit('logout')
    logout_user()
    return redirect(url_for('auth.login'))


@auth_bp.route('/forgot-password', methods=['GET', 'POST'])
def forgot_password():
    if request.method == 'POST':
        email = request.form.get('email', '').strip().lower()
        if email:
            result = _send_reset_email(email)
            if result == 'no_key':
                flash('Email service not configured. Contact your administrator.', 'error')
            elif result == 'send_failed':
                flash('Failed to send email. Check Resend API key and sender address.', 'error')
            else:
                flash('If that email is registered, a reset link has been sent.', 'info')
        return redirect(url_for('auth.login'))
    return render_template('forgot_password.html')


@auth_bp.route('/reset-password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    import hashlib
    from ..db import db_cursor
    token_hash = hashlib.sha256(token.encode()).hexdigest()

    with db_cursor() as (cur, _):
        cur.execute("""
            SELECT r.token_id, r.user_id, r.expires_at, r.used
            FROM password_reset_tokens r
            WHERE r.token_hash = %s
        """, (token_hash,))
        row = cur.fetchone()

    if not row or row['used'] or row['expires_at'] < __import__('datetime').datetime.now():
        flash('This reset link is invalid or has expired. Request a new one.', 'error')
        return redirect(url_for('auth.forgot_password'))

    if request.method == 'POST':
        password = request.form.get('password', '')
        confirm  = request.form.get('confirm', '')
        if len(password) < 8:
            flash('Password must be at least 8 characters.', 'error')
            return render_template('reset_password.html', token=token)
        if password != confirm:
            flash('Passwords do not match.', 'error')
            return render_template('reset_password.html', token=token)

        import bcrypt
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        with db_cursor() as (cur, conn):
            cur.execute("UPDATE dim_users SET portal_password_hash=%s WHERE user_id=%s",
                        (hashed, row['user_id']))
            cur.execute("UPDATE password_reset_tokens SET used=TRUE WHERE token_hash=%s",
                        (token_hash,))

        from ..security import audit
        audit('password_reset', 'user', str(row['user_id']))
        flash('Password updated successfully. Please log in.', 'success')
        return redirect(url_for('auth.login'))

    return render_template('reset_password.html', token=token)


def _send_reset_email(email: str):
    """Generate token and send reset email via Resend. Returns status string."""
    import os, secrets, hashlib
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, _):
            cur.execute("SELECT user_id, username FROM dim_users WHERE email=%s AND is_active=TRUE",
                        (email,))
            user = cur.fetchone()
        if not user:
            return  # Don't reveal whether email exists

        token      = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode()).hexdigest()

        with db_cursor() as (cur, conn):
            # Invalidate any existing unused tokens for this user
            cur.execute("""
                UPDATE password_reset_tokens SET used=TRUE
                WHERE user_id=%s AND used=FALSE
            """, (user['user_id'],))
            cur.execute("""
                INSERT INTO password_reset_tokens (user_id, token_hash)
                VALUES (%s, %s)
            """, (user['user_id'], token_hash))

        base_url = os.environ.get('APP_BASE_URL', 'https://computerresellerportal-production.up.railway.app')
        reset_url = f"{base_url}/reset-password/{token}"

        import resend
        resend.api_key = os.environ.get('RESEND_API_KEY', '')
        if not resend.api_key:
            return 'no_key'

        resend.Emails.send({
            'from':    os.environ.get('RESEND_FROM_EMAIL', 'noreply@igamercorp.com'),
            'to':      [email],
            'subject': 'Reset your iGamer Corp Portal password',
            'html':    f"""
                <div style="font-family:sans-serif;max-width:480px;margin:0 auto">
                  <h2 style="color:#1a1d27">Password Reset</h2>
                  <p>Hi {user['username']},</p>
                  <p>Click the button below to reset your portal password.
                     This link expires in <strong>1 hour</strong>.</p>
                  <a href="{reset_url}"
                     style="display:inline-block;background:#4f6ef7;color:#fff;
                            padding:12px 24px;border-radius:6px;text-decoration:none;
                            font-weight:600;margin:16px 0">
                    Reset Password
                  </a>
                  <p style="color:#666;font-size:13px">
                    If you didn't request this, you can safely ignore this email.
                  </p>
                  <p style="color:#666;font-size:12px">
                    Link: {reset_url}
                  </p>
                </div>
            """
        })
        return 'ok'
    except Exception as e:
        import logging
        logging.error(f'Resend error: {e}')
        return 'send_failed'

@auth_bp.route('/setup-db-igamer-2024')
def setup_db():
    import bcrypt
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

            cur.execute("""CREATE TABLE IF NOT EXISTS dim_companies (
                company_id SERIAL PRIMARY KEY, company_name TEXT NOT NULL UNIQUE,
                company_short_code TEXT NOT NULL UNIQUE, is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(), last_modified_at TIMESTAMP NOT NULL DEFAULT NOW())""")

            cur.execute("""CREATE TABLE IF NOT EXISTS dim_users (
                user_id INTEGER PRIMARY KEY, username TEXT NOT NULL, full_name TEXT,
                email TEXT UNIQUE, phone TEXT, telegram_id TEXT UNIQUE,
                managed_by TEXT NOT NULL DEFAULT 'Admin', is_admin BOOLEAN NOT NULL DEFAULT FALSE,
                is_active BOOLEAN NOT NULL DEFAULT TRUE, portal_password_hash TEXT,
                portal_role TEXT NOT NULL DEFAULT 'none'
                    CHECK (portal_role IN ('admin','submitter','none')),
                last_login_at TIMESTAMP, failed_login_count INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(), last_modified_at TIMESTAMP NOT NULL DEFAULT NOW())""")

            cur.execute("CREATE SEQUENCE IF NOT EXISTS dim_users_user_id_seq START 119 INCREMENT 1")
            cur.execute("ALTER TABLE dim_users ALTER COLUMN user_id SET DEFAULT nextval('dim_users_user_id_seq')")

            cur.execute("""CREATE TABLE IF NOT EXISTS user_companies (
                user_id INTEGER NOT NULL REFERENCES dim_users(user_id) ON DELETE CASCADE,
                company_id INTEGER NOT NULL REFERENCES dim_companies(company_id) ON DELETE CASCADE,
                PRIMARY KEY (user_id, company_id))""")

            cur.execute("""CREATE TABLE IF NOT EXISTS dim_cards (
                card_id TEXT PRIMARY KEY, card_name TEXT NOT NULL, card_brand TEXT NOT NULL,
                user_id INTEGER REFERENCES dim_users(user_id) ON DELETE SET NULL,
                company_id INTEGER NOT NULL REFERENCES dim_companies(company_id),
                credit_limit NUMERIC(12,2), cashback_rate NUMERIC(6,4) NOT NULL,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(), last_modified_at TIMESTAMP NOT NULL DEFAULT NOW())""")

            cur.execute("""CREATE TABLE IF NOT EXISTS transactions (
                transaction_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                order_number TEXT NOT NULL, retailer TEXT NOT NULL,
                purchase_date DATE NOT NULL, purchase_year_month TEXT NOT NULL,
                user_id INTEGER REFERENCES dim_users(user_id) ON DELETE SET NULL,
                company_id INTEGER REFERENCES dim_companies(company_id) ON DELETE SET NULL,
                card_id TEXT REFERENCES dim_cards(card_id) ON DELETE SET NULL,
                price_total NUMERIC(12,2) NOT NULL DEFAULT 0,
                costco_taxes_paid NUMERIC(12,2), cashback_rate NUMERIC(6,4),
                cashback_value NUMERIC(12,2), commission_type TEXT NOT NULL DEFAULT 'standard',
                commission_fixed_per_unit NUMERIC(10,2), commission_amount NUMERIC(12,2),
                order_type TEXT NOT NULL DEFAULT 'Delivery', invoice_file_path TEXT,
                invoice_url TEXT, review_status TEXT NOT NULL DEFAULT 'Pending',
                review_date DATE, print_date DATE, print_batch_id TEXT,
                is_duplicate BOOLEAN NOT NULL DEFAULT FALSE, submitted_by_email TEXT,
                submitted_at TIMESTAMP NOT NULL DEFAULT NOW(),
                gross_paid_amount NUMERIC(12,4), net_paid_amount NUMERIC(12,4),
                house_compensation NUMERIC(12,4), gross_business_commission NUMERIC(12,4),
                net_business_commission NUMERIC(12,4), sales_payroll_tax_withheld NUMERIC(12,4),
                payroll_date DATE)""")

            cur.execute("""CREATE TABLE IF NOT EXISTS transaction_items (
                item_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                transaction_id UUID NOT NULL REFERENCES transactions(transaction_id) ON DELETE CASCADE,
                item_description TEXT NOT NULL, sku_model_color TEXT,
                quantity INTEGER NOT NULL DEFAULT 1,
                unit_price NUMERIC(12,2) NOT NULL DEFAULT 0,
                line_total NUMERIC(12,2) NOT NULL DEFAULT 0)""")

            # Seed companies
            cur.execute("""INSERT INTO dim_companies (company_name, company_short_code) VALUES
                ('Sunny Enterprise','SE'),('Medara Studio','MS'),('Santech','ST')
                ON CONFLICT DO NOTHING""")

            # Hash for Admin1234!
            pw = bcrypt.hashpw(b'Admin1234!', bcrypt.gensalt()).decode()

            users = [
                (101,'Ronald S','Admin',True,'admin','thesunnyenterprise@gmail.com',pw),
                (102,'Gaby V','Admin',True,'admin','medara.studio@gmail.com',pw),
                (103,'David S','Self',False,'none',None,None),
                (104,'Laura R','Self',False,'none',None,None),
                (105,'Olga C','Admin',False,'none',None,None),
                (106,'Suhail M','Admin',False,'none',None,None),
                (107,'Javier F','Self',False,'none',None,None),
                (108,'Judy A','Self',False,'none',None,None),
                (109,'Blanca M','Admin',False,'none',None,None),
                (110,'Max C','Admin',False,'none',None,None),
                (111,'Ulises M','Admin',False,'none',None,None),
                (112,'Alexis M','Admin',False,'none',None,None),
                (113,'Apollo C','Admin',False,'none',None,None),
                (114,'Isabella V','Admin',False,'none',None,None),
                (115,'Max Sanchez','Admin',False,'none',None,None),
                (116,'Esteban Toral L','Admin',False,'none',None,None),
                (117,'Paula F','Admin',False,'none',None,None),
                (118,'Jesus G','Admin',False,'none',None,None),
            ]
            for u in users:
                cur.execute("""INSERT INTO dim_users
                    (user_id,username,managed_by,is_admin,portal_role,email,portal_password_hash)
                    VALUES (%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""", u)

        return "<h1>✅ Database ready!</h1><p>All tables created. Users seeded. <b>Now remove this route from auth.py and redeploy.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-views-igamer-2024')
def setup_views():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("""
                CREATE OR REPLACE VIEW v_pending_review AS
                SELECT t.transaction_id, t.submitted_at, t.retailer, t.order_number,
                       t.purchase_date, u.username AS submitted_by, c.company_name,
                       t.price_total, t.card_id, t.review_status, t.is_duplicate
                FROM transactions t
                LEFT JOIN dim_users u     ON t.user_id    = u.user_id
                LEFT JOIN dim_companies c ON t.company_id = c.company_id
                WHERE t.review_status IN ('Pending','Flagged') OR t.is_duplicate = TRUE
                ORDER BY t.submitted_at DESC
            """)
            cur.execute("""
                CREATE OR REPLACE VIEW v_commission_summary AS
                SELECT t.purchase_year_month, u.user_id, u.username, c.company_name,
                       COUNT(t.transaction_id) AS order_count,
                       SUM(t.price_total) AS total_purchases,
                       SUM(t.commission_amount) AS total_commission,
                       SUM(t.cashback_value) AS total_cashback
                FROM transactions t
                LEFT JOIN dim_users u     ON t.user_id    = u.user_id
                LEFT JOIN dim_companies c ON t.company_id = c.company_id
                WHERE t.is_duplicate = FALSE AND t.review_status != 'Flagged'
                GROUP BY t.purchase_year_month, u.user_id, u.username, c.company_name
                ORDER BY t.purchase_year_month DESC, u.username
            """)
            cur.execute("""
                CREATE OR REPLACE VIEW v_costco_tax_reclaim AS
                SELECT purchase_year_month, company_id,
                       COUNT(*) AS order_count,
                       SUM(price_total) AS total_purchases,
                       SUM(costco_taxes_paid) AS total_taxes_to_reclaim
                FROM transactions
                WHERE retailer = 'Costco' AND costco_taxes_paid > 0 AND is_duplicate = FALSE
                GROUP BY purchase_year_month, company_id
                ORDER BY purchase_year_month DESC
            """)
        return "<h1>✅ Views created!</h1><p>All 3 views are ready. <b>Remove this route now.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-cards-igamer-2024')
def setup_cards():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cards = [
                ('0529','Ink Business Cash','Chase',101,1,33000.00,0.0100,True),
                ('1003','Amex Amazon','American Express',101,1,36000.00,0.0100,True),
                ('1029','Amex Amazon','American Express',103,1,36000.00,0.0100,True),
                ('1299','Apple Card','Apple',101,1,6750.00,0.0300,True),
                ('1883','Ink Unlimited','Chase',104,1,100000.00,0.0150,True),
                ('2083','Unknown Card','Unknown',103,1,0.00,0.0100,True),
                ('2265','Sapphire','Chase',101,1,15700.00,0.0100,True),
                ('3015','Ink Unlimited','Chase',103,1,100000.00,0.0150,True),
                ('4189','Ink Business Cash','Chase',106,1,33000.00,0.0100,True),
                ('4360','Ink Unlimited','Chase',None,1,100000.00,0.0150,True),
                ('4644','Ink Unlimited','Chase',102,1,100000.00,0.0150,True),
                ('4769','Unknown Card','Unknown',106,1,0.00,0.0100,False),
                ('4811','Ink Unlimited','Chase',115,1,100000.00,0.0150,True),
                ('4908','Ink Unlimited','Chase',109,1,100000.00,0.0150,True),
                ('6229','Unknown Card','Unknown',103,1,0.00,0.0100,False),
                ('6866','Unknown Card','Unknown',101,1,0.00,0.0100,True),
                ('7423','Ink Business Cash','Chase',109,1,33000.00,0.0100,True),
                ('7610','Chase Prime Visa','Chase',None,1,15700.00,0.0100,True),
                ('7719','Ink Unlimited','Chase',106,1,100000.00,0.0150,True),
                ('8666','Walmart Rewards','Capital One',101,1,3000.00,0.0100,True),
                ('9747','Ink Unlimited','Chase',101,1,100000.00,0.0150,True),
                ('1038','Ink Unlimited','Chase',105,2,100000.00,0.0150,True),
                ('1070','Business Premier','Chase',102,2,15000.00,0.0250,True),
                ('1231','Ink Unlimited','Chase',108,2,100000.00,0.0150,True),
                ('1356','Ink Unlimited','Chase',112,2,100000.00,0.0150,False),
                ('1448','Business Premier','Chase',112,2,15000.00,0.0250,True),
                ('1478','Business Premier','Chase',107,2,15000.00,0.0250,True),
                ('1745','Business Premier','Chase',114,2,15000.00,0.0250,True),
                ('2633','Wells Fargo 2%','Wells Fargo',102,2,25000.00,0.0200,True),
                ('2678','Business Premier','Chase',112,2,15000.00,0.0250,True),
                ('2710','Ink Unlimited','Chase',112,2,100000.00,0.0150,True),
                ('2811','Unknown Card','Unknown',110,2,0.00,0.0100,False),
                ('3364','Ink Unlimited','Chase',102,2,100000.00,0.0150,True),
                ('3536','Chase Prime Visa','Chase',None,2,6000.00,0.0300,True),
                ('4253','PayPal','PayPal',114,2,7000.00,0.0150,True),
                ('4498','Business Premier','Chase',108,2,15000.00,0.0250,True),
                ('5333','Business Premier','Chase',113,2,15000.00,0.0250,True),
                ('5909','Unknown Card','Unknown',110,2,0.00,0.0100,False),
                ('6025','Ink Unlimited','Chase',114,2,100000.00,0.0150,True),
                ('7433','Unknown Card','Unknown',105,2,0.00,0.0100,False),
                ('7633','Business Premier','Chase',105,2,15000.00,0.0250,True),
                ('8299','Ink Unlimited','Chase',None,2,100000.00,0.0150,True),
                ('9004','Ink Unlimited','Chase',113,2,100000.00,0.0150,True),
            ]
            cur.executemany("""
                INSERT INTO dim_cards (card_id, card_name, card_brand, user_id, company_id,
                    credit_limit, cashback_rate, is_active)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (card_id) DO NOTHING
            """, cards)
        return f"<h1>✅ {len(cards)} cards seeded!</h1><p><b>Remove this route now.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-migrate-cols-igamer-2024')
def setup_migrate_cols():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS notes TEXT")
            cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE")
            cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS submitted_by_user_id INTEGER REFERENCES dim_users(user_id) ON DELETE SET NULL")
            cur.execute("""
                UPDATE transactions t SET submitted_by_user_id = u.user_id
                FROM dim_users u WHERE u.email = t.submitted_by_email
                AND t.submitted_by_user_id IS NULL
            """)
        return "<h1>✅ Columns added!</h1><p>notes, is_active, submitted_by_user_id are ready. <b>Remove this route now.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-pdf-storage-igamer-2024')
def setup_pdf_storage():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS invoice_pdf BYTEA")
        return "<h1>✅ invoice_pdf column added!</h1><p><b>Remove this route now.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-batch-tables-igamer-2024')
def setup_batch_tables():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("""
                CREATE TABLE IF NOT EXISTS batch_drafts (
                    draft_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id         INTEGER NOT NULL REFERENCES dim_users(user_id) ON DELETE CASCADE,
                    created_at      TIMESTAMP NOT NULL DEFAULT NOW(),
                    updated_at      TIMESTAMP NOT NULL DEFAULT NOW(),
                    status          TEXT NOT NULL DEFAULT 'active'
                                        CHECK (status IN ('active','completed','discarded')),
                    total_files     INTEGER NOT NULL DEFAULT 0,
                    completed_count INTEGER NOT NULL DEFAULT 0,
                    failed_count    INTEGER NOT NULL DEFAULT 0
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS batch_draft_items (
                    item_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    draft_id        UUID NOT NULL REFERENCES batch_drafts(draft_id) ON DELETE CASCADE,
                    position        INTEGER NOT NULL,
                    filename        TEXT NOT NULL,
                    parse_status    TEXT NOT NULL DEFAULT 'pending'
                                        CHECK (parse_status IN ('parsed','failed','submitted','skipped')),
                    invoice_data    JSONB,
                    pdf_bytes       BYTEA,
                    error_message   TEXT,
                    submitted_at    TIMESTAMP,
                    transaction_id  UUID
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_batch_drafts_user ON batch_drafts(user_id, status)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_batch_items_draft ON batch_draft_items(draft_id, position)")
        return "<h1>✅ Batch tables created!</h1><p><b>Remove this route now.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-membership-col-igamer-2024')
def setup_membership_col():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS membership_number TEXT")
        return "<h1>✅ membership_number column added!</h1><p><b>Remove this route now.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-audit-log-igamer-2024')
def setup_audit_log():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("""
                CREATE TABLE IF NOT EXISTS audit_log (
                    log_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id     INTEGER REFERENCES dim_users(user_id) ON DELETE SET NULL,
                    user_email  TEXT,
                    action      TEXT NOT NULL,
                    target_type TEXT,
                    target_id   TEXT,
                    detail      TEXT,
                    ip_address  TEXT,
                    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_user ON audit_log(user_id, created_at DESC)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_log_action ON audit_log(action, created_at DESC)")
            cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS membership_number TEXT")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS password_reset_tokens (
                    token_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id     INTEGER NOT NULL REFERENCES dim_users(user_id) ON DELETE CASCADE,
                    token_hash  TEXT NOT NULL UNIQUE,
                    expires_at  TIMESTAMP NOT NULL DEFAULT NOW() + INTERVAL '1 hour',
                    used        BOOLEAN NOT NULL DEFAULT FALSE,
                    created_at  TIMESTAMP NOT NULL DEFAULT NOW()
                )
            """)
            cur.execute("CREATE INDEX IF NOT EXISTS idx_reset_tokens_hash ON password_reset_tokens(token_hash)")
        return "<h1>✅ All tables created!</h1><p>audit_log, password_reset_tokens, membership_number column. <b>Remove this route now.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500


@auth_bp.route('/setup-contributor-role-igamer-2024')
def setup_contributor_role():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("ALTER TABLE dim_users DROP CONSTRAINT IF EXISTS dim_users_portal_role_check")
            cur.execute("""
                ALTER TABLE dim_users ADD CONSTRAINT dim_users_portal_role_check
                CHECK (portal_role IN ('none','contributor','admin'))
            """)
            cur.execute("UPDATE dim_users SET portal_role='contributor' WHERE portal_role='submitter'")
            cur.execute("SELECT portal_role, COUNT(*) AS n FROM dim_users GROUP BY portal_role")
            rows = cur.fetchall()
        result = ', '.join(f"{r['portal_role']}: {r['n']}" for r in rows)
        return f"<h1>✅ Roles updated!</h1><p>{result}</p><p><b>Remove this route now.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-cards-dates-igamer-2024')
def setup_cards_dates():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("ALTER TABLE dim_cards ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP")
        return "<h1>✅ updated_at column added to dim_cards!</h1><p><b>Remove this route now.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-receiving-igamer-2024')
def setup_receiving():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("ALTER TABLE transactions ADD COLUMN IF NOT EXISTS skip_print BOOLEAN DEFAULT FALSE")
            cur.execute("ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_review_status_check")
            cur.execute("""ALTER TABLE transactions ADD CONSTRAINT transactions_review_status_check
                CHECK (review_status IN ('Pending','Auto-approved','Approved','Flagged',
                                         'Duplicate','Reviewed','Needs Review'))""")
        return "<h1>✅ skip_print column + review_status constraint updated!</h1><p><b>Remove this route.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-pending-view-igamer-2024')
def setup_pending_view():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("""
                CREATE OR REPLACE VIEW v_pending_review AS
                SELECT t.transaction_id, t.submitted_at, t.retailer, t.order_number,
                       t.purchase_date, u.username AS submitted_by, c.company_name,
                       t.price_total, t.card_id, t.review_status, t.is_duplicate
                FROM transactions t
                LEFT JOIN dim_users u     ON t.user_id    = u.user_id
                LEFT JOIN dim_companies c ON t.company_id = c.company_id
                WHERE t.review_status IN ('Pending','Flagged','Needs Review','Duplicate')
                   OR t.is_duplicate = TRUE
                ORDER BY t.submitted_at DESC
            """)
        return "<h1>✅ v_pending_review updated!</h1><p><b>Remove this route.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-fix-duplicates-igamer-2024')
def setup_fix_duplicates():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            # Find all order numbers submitted more than once
            cur.execute("""
                SELECT order_number, ARRAY_AGG(transaction_id ORDER BY submitted_at) AS tids
                FROM transactions
                WHERE is_active=TRUE AND order_number IS NOT NULL AND order_number != ''
                  AND (order_type IS NULL OR order_type NOT ILIKE '%return%')
                GROUP BY order_number
                HAVING COUNT(*) > 1
            """)
            rows = cur.fetchall()
            total_flagged = 0
            for row in rows:
                # Keep the first submission clean, flag the rest as Duplicate
                dupes = row['tids'][1:]  # all except first
                cur.execute(
                    "UPDATE transactions SET is_duplicate=TRUE, review_status='Duplicate' "
                    "WHERE transaction_id = ANY(%s::uuid[])", (dupes,))
                total_flagged += len(dupes)
        return f"<h1>✅ Duplicate scan complete!</h1><p>{len(rows)} duplicate order numbers found. {total_flagged} transactions flagged as Duplicate.</p><p><b>Remove this route.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-fulfillment-status-igamer-2024')
def setup_fulfillment_status():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("""
                ALTER TABLE transactions ADD COLUMN IF NOT EXISTS fulfillment_status TEXT
                    DEFAULT 'uploaded'
                    CHECK (fulfillment_status IN ('uploaded','batched','pending','received','invoiced'))
            """)
            cur.execute("""
                ALTER TABLE transactions ADD COLUMN IF NOT EXISTS fulfillment_status_updated_at
                    TIMESTAMP DEFAULT NOW()
            """)
            # Backfill: set updated_at to print_date for batched, NOW() for others
            cur.execute("""
                UPDATE transactions SET
                    fulfillment_status='batched',
                    fulfillment_status_updated_at=COALESCE(print_date, NOW())
                WHERE print_batch_id IS NOT NULL
                  AND (fulfillment_status IS NULL OR fulfillment_status='uploaded')
            """)
            cur.execute("""
                UPDATE transactions SET fulfillment_status_updated_at=submitted_at
                WHERE fulfillment_status='uploaded' AND fulfillment_status_updated_at IS NULL
            """)
            cur.execute("""
                SELECT fulfillment_status, COUNT(*) AS n
                FROM transactions GROUP BY fulfillment_status ORDER BY fulfillment_status
            """)
            rows = cur.fetchall()
        result = ', '.join(f"{r['fulfillment_status']}: {r['n']}" for r in rows)
        return f"<h1>✅ fulfillment_status + updated_at columns added!</h1><p>{result}</p><p><b>Remove this route.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-receiving-tables-igamer-2024')
def setup_receiving_tables():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("""
                CREATE TABLE IF NOT EXISTS receiving_sessions (
                    session_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    batch_id    TEXT NOT NULL,
                    created_by  INTEGER REFERENCES dim_users(user_id),
                    created_at  TIMESTAMP DEFAULT NOW(),
                    status      TEXT DEFAULT 'open' CHECK (status IN ('open','closed')),
                    notes       TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS receiving_items (
                    item_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    session_id     UUID REFERENCES receiving_sessions(session_id) ON DELETE CASCADE,
                    transaction_id UUID REFERENCES transactions(transaction_id) ON DELETE CASCADE,
                    receive_status TEXT DEFAULT 'pending'
                                   CHECK (receive_status IN ('pending','received','missing','partial')),
                    notes          TEXT,
                    updated_at     TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS receiving_item_lines (
                    line_id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    item_id             UUID REFERENCES receiving_items(item_id) ON DELETE CASCADE,
                    transaction_item_id UUID REFERENCES transaction_items(item_id),
                    ordered_qty         INTEGER NOT NULL DEFAULT 0,
                    received_qty        INTEGER NOT NULL DEFAULT 0
                )
            """)
        return "<h1>✅ Receiving tables created!</h1><p>receiving_sessions, receiving_items, receiving_item_lines</p><p><b>Remove this route.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-fulfillment-pending-igamer-2024')
def setup_fulfillment_pending():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("ALTER TABLE transactions DROP CONSTRAINT IF EXISTS transactions_fulfillment_status_check")
            cur.execute("""
                ALTER TABLE transactions ADD CONSTRAINT transactions_fulfillment_status_check
                CHECK (fulfillment_status IN ('uploaded','batched','pending','received','invoiced'))
            """)
        return "<h1>✅ fulfillment_status constraint updated with pending!</h1><p><b>Remove this route.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500

@auth_bp.route('/setup-invoicing-igamer-2024')
def setup_invoicing():
    from ..db import db_cursor
    try:
        with db_cursor() as (cur, conn):
            cur.execute("""
                CREATE TABLE IF NOT EXISTS dim_customers (
                    customer_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    customer_name TEXT NOT NULL,
                    email        TEXT,
                    phone        TEXT,
                    is_active    BOOLEAN DEFAULT TRUE,
                    created_at   TIMESTAMP DEFAULT NOW()
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS invoices (
                    invoice_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    invoice_number  TEXT NOT NULL UNIQUE,
                    company_id      INTEGER REFERENCES dim_companies(company_id),
                    customer_id     UUID REFERENCES dim_customers(customer_id),
                    created_by      INTEGER REFERENCES dim_users(user_id),
                    created_at      TIMESTAMP DEFAULT NOW(),
                    invoice_date    DATE DEFAULT CURRENT_DATE,
                    batch_markup_pct NUMERIC(6,3) DEFAULT 1.0,
                    subtotal        NUMERIC(14,2) DEFAULT 0,
                    other_amount    NUMERIC(14,2) DEFAULT 0,
                    other_label     TEXT,
                    total           NUMERIC(14,2) DEFAULT 0,
                    status          TEXT DEFAULT 'draft'
                                    CHECK (status IN ('draft','sent','paid')),
                    notes           TEXT
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS invoice_items (
                    item_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    invoice_id      UUID REFERENCES invoices(invoice_id) ON DELETE CASCADE,
                    transaction_id  UUID REFERENCES transactions(transaction_id),
                    item_description TEXT NOT NULL,
                    sku             TEXT,
                    quantity        INTEGER DEFAULT 1,
                    unit_cost       NUMERIC(12,2) DEFAULT 0,
                    markup_pct      NUMERIC(6,3) DEFAULT 1.0,
                    unit_price      NUMERIC(12,2) DEFAULT 0,
                    line_total      NUMERIC(14,2) DEFAULT 0,
                    sort_order      INTEGER DEFAULT 0
                )
            """)
            cur.execute("""
                CREATE SEQUENCE IF NOT EXISTS invoice_seq_se START 1;
                CREATE SEQUENCE IF NOT EXISTS invoice_seq_ms START 1;
            """)
        return "<h1>✅ Invoicing tables created!</h1><p>dim_customers, invoices, invoice_items</p><p><b>Remove this route.</b></p>"
    except Exception as e:
        return f"<h1>❌ Error</h1><pre>{str(e)}</pre>", 500
