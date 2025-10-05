import sqlite3
from .datamart_base import Datamart

class DatamartSQLite(Datamart):
    def __init__(self, db_path: str = "datamart.sqlite"):
        self.conn = sqlite3.connect(db_path)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS books (
                book_id     INTEGER PRIMARY KEY,
                title       TEXT,
                author      TEXT,
                language    TEXT,
                header_path TEXT,
                body_path   TEXT,
                ingested_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_author ON books(author)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_title  ON books(title)")
        self.conn.commit()

    def upsert_many(self, rows):
        cur = self.conn.cursor()
        cur.executemany("""
            INSERT INTO books(book_id,title,author,language,header_path,body_path)
            VALUES (:book_id,:title,:author,:language,:header_path,:body_path)
            ON CONFLICT(book_id) DO UPDATE SET
              title=excluded.title, author=excluded.author, language=excluded.language,
              header_path=excluded.header_path, body_path=excluded.body_path
        """, list(rows))
        self.conn.commit()
        return cur.rowcount

    def get_by_author(self, author: str) -> list[dict]:
        cur = self.conn.execute("SELECT * FROM books WHERE author=?", (author,))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def get_by_title(self, title: str) -> list[dict]:
        cur = self.conn.execute("SELECT * FROM books WHERE title=?", (title,))
        cols = [c[0] for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

    def close(self) -> None:
        self.conn.close()
