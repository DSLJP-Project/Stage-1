from storage.datalakes.datalake_tria import Datalake
from storage.datalakes.datalake_sql import DatalakeSQL
from utils.API import API
import re

START_RE = re.compile(r'\*\*\*\s*START OF (THIS|THE) PROJECT GUTENBERG EBOOK.*', re.IGNORECASE)
END_RE   = re.compile(r'\*\*\*\s*END OF (THIS|THE) PROJECT GUTENBERG EBOOK.*', re.IGNORECASE)

datalake_tria = Datalake(root="datalake_tria")
datalake_sql  = DatalakeSQL(db_path="datalake_sql/books.db")
api = API()

def _split_header_body(raw_text: str) -> tuple[str, str, str]:
    t = raw_text.replace('\r\n', '\n').replace('\r', '\n').lstrip('\ufeff')
    sm = START_RE.search(t)
    em = END_RE.search(t)
    if sm and em and sm.end() < em.start():
        header = t[:sm.end()].strip()
        body   = t[sm.end():em.start()].strip()
        footer = t[em.start():].strip()
        return header, body, footer
    return "", t.strip(), ""

def ingest_book(book_id: int):
    raw = api.fetch_gutenberg_text(book_id)
    if not raw:
        print(f"[WARN] Book {book_id}: no descargado como texto plano.")
        return

    info_tria = datalake_tria.save_raw(book_id, raw)
    header, body, footer = _split_header_body(raw)
    datalake_sql.save_raw(book_id, header, body)
    print(f"[INFO] Book {book_id} -> FS(OK) SQL(OK) [{info_tria['date']}/{info_tria['hour']}]")

if __name__ == "__main__":
    for book_id in [11, 84, 98, 1661, 2701, 1342, 1952, 4300]:
        ingest_book(book_id)
