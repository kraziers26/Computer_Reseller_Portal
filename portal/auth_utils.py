from functools import wraps
from flask import redirect, url_for, flash
from flask_login import current_user


def require_role(role):
    """
    Role hierarchy: admin > contributor > none
    require_role('contributor') allows both contributor and admin.
    require_role('admin') allows admin only.
    """
    def decorator(f):
        @wraps(f)
        def decorated(*args, **kwargs):
            if not current_user.is_authenticated:
                return redirect(url_for('auth.login'))
            if role == 'admin' and current_user.portal_role != 'admin':
                flash('Admin access required.', 'error')
                return redirect(url_for('upload.upload'))
            if role == 'contributor' and current_user.portal_role not in ('contributor', 'admin'):
                flash('Access denied.', 'error')
                return redirect(url_for('auth.login'))
            return f(*args, **kwargs)
        return decorated
    return decorator
