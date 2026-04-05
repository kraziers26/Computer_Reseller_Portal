import os
import psycopg2
import psycopg2.extras
from contextlib import contextmanager

DB_CONFIG = {
    'dbname':   os.environ.get('DB_NAME',     'igamer_corp'),
    'user':     os.environ.get('DB_USER',     'migrator'),
    'password': os.environ.get('DB_PASSWORD', 'migrate123'),
    'host':     os.environ.get('DB_HOST',     'localhost'),
    'port':     os.environ.get('DB_PORT',     '5432'),
}

def get_db():
    return psycopg2.connect(**DB_CONFIG)

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
