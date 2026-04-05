from functools import wraps
from flask import abort
from flask_login import current_user

def require_role(role):
    """Decorator to enforce portal_role access.
    role: 'admin' or 'submitter'
    'submitter' allows both submitters and admins.
    'admin' allows only admins.
    """
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not current_user.is_authenticated:
                from flask import redirect, url_for
                return redirect(url_for('auth.login'))
            if role == 'admin' and not current_user.is_admin:
                abort(403)
            if role == 'submitter' and not current_user.is_submitter:
                abort(403)
            return f(*args, **kwargs)
        return decorated
    return decorator
