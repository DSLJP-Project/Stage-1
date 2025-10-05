import sqlite3
from pathlib import Path
from datetime import datetime

class DatalakeSQL:
    def __init__(self, db_path: str = "datalake_sql/books.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self._create_table()

    def _create_table(self):
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS books (
                    book_id INTEGER PRIMARY KEY,
                    header TEXT,
                    body TEXT,
                    ingestion_time TEXT
                )
            """)

    def save_raw(self, book_id: int, header: str, body: str, dt: datetime | None = None):
        dt = dt or datetime.utcnow()
        ingestion_time = dt.isoformat()
        with self.conn:
            self.conn.execute("""
                INSERT OR REPLACE INTO books (book_id, header, body, ingestion_time)
                VALUES (?, ?, ?, ?)
            """, (book_id, header, body, ingestion_time))
        return {"book_id": book_id, "ingestion_time": ingestion_time}

    def get_book(self, book_id: int):
        cur = self.conn.cursor()
        cur.execute("SELECT book_id, header, body, ingestion_time FROM books WHERE book_id = ?", (book_id,))
        return cur.fetchone()

    def iter_books(self):
        cur = self.conn.cursor()
        cur.execute("SELECT book_id, header, body, ingestion_time FROM books")
        for row in cur.fetchall():
            yield row
