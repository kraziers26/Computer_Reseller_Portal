import os
import psycopg
from psycopg.rows import dict_row
from contextlib import contextmanager

def get_db():
    database_url = os.environ.get('DATABASE_URL')
    if database_url:
        return psycopg.connect(database_url, row_factory=dict_row)
    return psycopg.connect(
        dbname   = os.environ.get('DB_NAME',     'igamer_corp'),
        user     = os.environ.get('DB_USER',     'postgres'),
        password = os.environ.get('DB_PASSWORD', ''),
        host     = os.environ.get('DB_HOST',     'localhost'),
        port     = os.environ.get('DB_PORT',     '5432'),
        row_factory=dict_row,
    )

@contextmanager
def db_cursor(dict_cursor=True):
    conn = get_db()
    try:
        cur = conn.cursor()
        yield cur, conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
