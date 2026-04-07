"""
Security utilities for the ComputerReseller Invoices Portal.
Centralises: rate limiting, CSRF, security headers, audit logging,
session timeout, and login-attempt lockout.
"""
import os
from datetime import timedelta
from flask import request, session, redirect, url_for, flash, g
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf.csrf import CSRFProtect
from flask_talisman import Talisman

# ── Instances (init_app called in create_app) ─────────────────────────────────
limiter  = Limiter(key_func=get_remote_address, default_limits=[])
csrf     = CSRFProtect()

# ── Security headers ──────────────────────────────────────────────────────────
CSP = {
    'default-src': ["'self'"],
    'script-src':  ["'self'", "'unsafe-inline'"],   # needed for inline JS in templates
    'style-src':   ["'self'", "'unsafe-inline'"],
    'img-src':     ["'self'", 'data:'],
    'frame-src':   ["'self'"],                       # for PDF iframes
    'font-src':    ["'self'", 'data:'],
    'object-src':  ["'none'"],
}


def init_security(app):
    """Call once inside create_app after blueprints are registered."""

    # Rate limiter — uses in-memory storage (fine for single-dyno Railway)
    limiter.init_app(app)

    # CSRF — protects all POST forms automatically
    csrf.init_app(app)

    # Security headers
    Talisman(
        app,
        force_https=False,           # Railway handles HTTPS termination
        strict_transport_security=False,
        content_security_policy=CSP,
        content_security_policy_nonce_in=['script-src'],
        frame_options='SAMEORIGIN',  # allow PDF iframes from same origin
        referrer_policy='strict-origin-when-cross-origin',
    )

    # Session timeout — 60 minutes of inactivity
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=60)

    @app.before_request
    def enforce_session_timeout():
        """Expire session after PERMANENT_SESSION_LIFETIME of inactivity."""
        session.permanent = True
        # Skip static files and login page
        if request.endpoint in ('static', 'auth.login', 'auth.logout', None):
            return
        # Check public setup routes
        if request.endpoint and request.endpoint.startswith('auth.setup'):
            return

    @app.after_request
    def add_extra_headers(response):
        """Extra headers not covered by Talisman."""
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Permissions-Policy']     = 'geolocation=(), microphone=(), camera=()'
        response.headers['Cache-Control']          = 'no-store, no-cache, must-revalidate'
        return response


# ── Audit log helper ──────────────────────────────────────────────────────────
def audit(action, target_type=None, target_id=None, detail=None):
    """Write an audit log entry. Call from any route."""
    from .db import db_cursor
    from flask_login import current_user
    try:
        user_id    = current_user.id    if current_user.is_authenticated else None
        user_email = current_user.email if current_user.is_authenticated else None
        ip         = request.headers.get('X-Forwarded-For', request.remote_addr or '')
        if ip:
            ip = ip.split(',')[0].strip()
        with db_cursor() as (cur, _):
            cur.execute("""
                INSERT INTO audit_log
                    (user_id, user_email, action, target_type, target_id, detail, ip_address)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (user_id, user_email, action, target_type, target_id, detail, ip))
    except Exception:
        pass   # never crash the app due to audit failure


# ── Login lockout (stored in DB for persistence across restarts) ──────────────
MAX_ATTEMPTS  = 5
LOCKOUT_MINS  = 15


def check_login_lockout(email: str) -> tuple[bool, int]:
    """Returns (is_locked, minutes_remaining). Uses audit_log for tracking."""
    from .db import db_cursor
    try:
        with db_cursor() as (cur, _):
            # Count failed attempts in last LOCKOUT_MINS minutes
            cur.execute("""
                SELECT COUNT(*) AS n,
                       EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))/60 AS age_mins
                FROM audit_log
                WHERE action     = 'login_failed'
                  AND detail     = %s
                  AND created_at > NOW() - INTERVAL '15 minutes'
            """, (email.lower(),))
            row = cur.fetchone()
            count = row['n'] or 0
            if count >= MAX_ATTEMPTS:
                remaining = max(0, int(LOCKOUT_MINS - (row['age_mins'] or 0)))
                return True, remaining
    except Exception:
        pass
    return False, 0


def record_failed_login(email: str):
    audit('login_failed', target_type='user', detail=email.lower())


def record_successful_login(email: str):
    audit('login_success', target_type='user', detail=email.lower())
