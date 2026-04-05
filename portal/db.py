import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

def get_db():
    # Railway provides a single DATABASE_URL — use it if available
    database_url = os.environ.get('DATABASE_URL')
    if database_url:
        return psycopg2.connect(database_url)

    # Fallback to individual variables for local dev
    return psycopg2.connect(
        dbname   = os.environ.get('DB_NAME',     'igamer_corp'),
        user     = os.environ.get('DB_USER',     'postgres'),
        password = os.environ.get('DB_PASSWORD', ''),
        host     = os.environ.get('DB_HOST',     'localhost'),
        port     = os.environ.get('DB_PORT',     '5432'),
    )

@contextmanager
def db_cursor(dict_cursor=True):
    conn = get_db()
    try:
        factory = psycopg2.extras.RealDictCursor if dict_cursor else None
        cur = conn.cursor(cursor_factory=factory)
        yield cur, conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
