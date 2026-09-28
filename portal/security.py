"""
Security utilities for the ComputerReseller Invoices Portal.
"""
import os
from datetime import timedelta
from flask import request, session, Response
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_wtf.csrf import CSRFProtect
from flask_talisman import Talisman

limiter = Limiter(key_func=get_remote_address, default_limits=[])
csrf    = CSRFProtect()

# CSP — unsafe-inline needed for all inline <script> and <style> in templates
# No nonces (would require injecting nonce into every template tag)
CSP = {
    'default-src': ["'self'"],
    'script-src':  ["'self'", "'unsafe-inline'"],
    'style-src':   ["'self'", "'unsafe-inline'"],
    'img-src':     ["'self'", 'data:'],
    'frame-src':   ["'self'"],
    'font-src':    ["'self'", 'data:'],
    'object-src':  ["'none'"],
    'base-uri':    ["'self'"],
    'form-action': ["'self'"],
}


def init_staging_gate(app):
    """
    HTTP Basic Auth in front of the entire app — staging only.

    Activates only when STAGING_BASIC_AUTH_USER and STAGING_BASIC_AUTH_PASS
    are both set. Leave them unset in production; their absence there is
    what keeps production gate-free.
    """
    auth_user = os.environ.get('STAGING_BASIC_AUTH_USER')
    auth_pass = os.environ.get('STAGING_BASIC_AUTH_PASS')

    if not (auth_user and auth_pass):
        return  # not staging — no gate applied

    @app.before_request
    def _require_staging_auth():
        if request.endpoint == 'static':
            return  # let CSS/JS/images through without prompting
        auth = request.authorization
        if not auth or auth.username != auth_user or auth.password != auth_pass:
            return Response(
                'Authentication required', 401,
                {'WWW-Authenticate': 'Basic realm="Staging"'}
            )


def init_security(app):
    init_staging_gate(app)

    limiter.init_app(app)
    csrf.init_app(app)

    Talisman(
        app,
        force_https=False,               # Railway handles HTTPS termination
        strict_transport_security=False,
        content_security_policy=CSP,
        content_security_policy_nonce_in=[],  # NO nonces — use unsafe-inline instead
        frame_options='SAMEORIGIN',           # allow PDF iframes
        referrer_policy='strict-origin-when-cross-origin',
    )

    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(minutes=60)

    @app.after_request
    def add_extra_headers(response):
        response.headers['X-Content-Type-Options'] = 'nosniff'
        response.headers['Permissions-Policy']     = 'geolocation=(), microphone=(), camera=()'
        # Don't cache authenticated pages
        if request.endpoint not in ('static', None):
            response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
        return response


def audit(action, target_type=None, target_id=None, detail=None):
    """Write an audit log entry. Silent failure — never crashes the app."""
    from .db import db_cursor
    from flask_login import current_user
    try:
        user_id    = current_user.id    if current_user.is_authenticated else None
        user_email = current_user.email if current_user.is_authenticated else None
        ip = request.headers.get('X-Forwarded-For', request.remote_addr or '')
        if ip:
            ip = ip.split(',')[0].strip()
        with db_cursor() as (cur, _):
            cur.execute("""
                INSERT INTO audit_log
                    (user_id, user_email, action, target_type, target_id, detail, ip_address)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (user_id, user_email, action, target_type, target_id, detail, ip))
    except Exception:
        pass


MAX_ATTEMPTS = 5
LOCKOUT_MINS = 15


def check_login_lockout(email: str):
    """Returns (is_locked, minutes_remaining)."""
    from .db import db_cursor
    try:
        with db_cursor() as (cur, _):
            cur.execute("""
                SELECT COUNT(*) AS n,
                       EXTRACT(EPOCH FROM (NOW() - MIN(created_at)))/60 AS age_mins
                FROM audit_log
                WHERE action     = 'login_failed'
                  AND detail     = %s
                  AND created_at > NOW() - INTERVAL '15 minutes'
            """, (email.lower(),))
            row = cur.fetchone()
            count = int(row['n'] or 0)
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
