import bcrypt
from flask_login import UserMixin
from .db import db_cursor

class User(UserMixin):
    def __init__(self, data):
        self.id           = data['user_id']
        self.username     = data['username']
        self.email        = data['email']
        self.portal_role  = data['portal_role']
        self.is_active_   = data['is_active']
        self.company_ids  = data.get('company_ids', [])

    def get_id(self):
        return str(self.id)

    @property
    def is_admin(self):
        return self.portal_role == 'admin'

    @property
    def is_contributor(self):
        return self.portal_role in ('admin', 'contributor')

    @property
    def is_active(self):
        return self.is_active_

    @staticmethod
    def get_by_id(user_id):
        with db_cursor() as (cur, _):
            cur.execute("""
                SELECT u.*, ARRAY_AGG(uc.company_id) AS company_ids
                FROM dim_users u
                LEFT JOIN user_companies uc ON u.user_id = uc.user_id
                WHERE u.user_id = %s
                GROUP BY u.user_id
            """, (user_id,))
            row = cur.fetchone()
        return User(row) if row else None

    @staticmethod
    def get_by_email(email):
        with db_cursor() as (cur, _):
            cur.execute("""
                SELECT u.*, ARRAY_AGG(uc.company_id) AS company_ids
                FROM dim_users u
                LEFT JOIN user_companies uc ON u.user_id = uc.user_id
                WHERE LOWER(u.email) = LOWER(%s) AND u.is_active = TRUE
                  AND u.portal_role != 'none'
                GROUP BY u.user_id
            """, (email,))
            row = cur.fetchone()
        return User(row) if row else None

    def check_password(self, password):
        """Instance method — check password against stored hash."""
        with db_cursor() as (cur, _):
            cur.execute(
                "SELECT portal_password_hash FROM dim_users WHERE user_id = %s",
                (self.id,)
            )
            row = cur.fetchone()
        if not row or not row['portal_password_hash']:
            return False
        return bcrypt.checkpw(password.encode(), row['portal_password_hash'].encode())

    @staticmethod
    def set_password(user_id, password):
        hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
        with db_cursor() as (cur, conn):
            cur.execute(
                "UPDATE dim_users SET portal_password_hash = %s WHERE user_id = %s",
                (hashed, user_id)
            )

    @staticmethod
    def record_login(user_id):
        with db_cursor() as (cur, conn):
            cur.execute(
                "UPDATE dim_users SET last_login_at = NOW(), failed_login_count = 0 "
                "WHERE user_id = %s", (user_id,)
            )

    @staticmethod
    def record_failed_login(email):
        with db_cursor() as (cur, conn):
            cur.execute(
                "UPDATE dim_users SET failed_login_count = failed_login_count + 1 "
                "WHERE LOWER(email) = LOWER(%s)", (email,)
            )
