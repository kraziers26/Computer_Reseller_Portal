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
    if current_user.is_authenticated:
        return redirect(url_for('auth.index'))

    if request.method == 'POST':
        email    = request.form.get('email', '').strip()
        password = request.form.get('password', '')

        if not email or not password:
            flash('Email and password are required.', 'error')
            return render_template('login.html')

        if User.check_password(email, password):
            user = User.get_by_email(email)
            if user:
                login_user(user, remember=True)
                User.record_login(user.id)
                if user.is_admin:
                    return redirect(url_for('admin.dashboard'))
                return redirect(url_for('upload.upload'))
        else:
            User.record_failed_login(email)
            flash('Invalid email or password.', 'error')

    return render_template('login.html')

@auth_bp.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('auth.login'))

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
