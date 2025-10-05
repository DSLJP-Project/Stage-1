from pathlib import Path
from datetime import datetime

START_MARKER = "*** START OF THE PROJECT GUTENBERG EBOOK"
END_MARKER   = "*** END OF THE PROJECT GUTENBERG EBOOK"

class Datalake:
    def __init__(self, root: str = "datalake"):
        self.root = Path(root)

    def _now_parts(self, dt: datetime | None = None):
        dt = dt or datetime.utcnow()
        return dt.strftime("%Y%m%d"), dt.strftime("%H")

    def save_raw(self, book_id: int | str, raw_text: str, dt: datetime | None = None) -> dict:
        ymd, hh = self._now_parts(dt)
        base = self.root / ymd / hh
        base.mkdir(parents=True, exist_ok=True)

        header = ""
        body = raw_text
        if START_MARKER in raw_text and END_MARKER in raw_text:
            header_raw, rest = raw_text.split(START_MARKER, 1)
            body_raw, _ = rest.split(END_MARKER, 1)
            header = header_raw.strip()
            body = body_raw.strip()

        header_path = base / f"{book_id}_header.txt"
        body_path   = base / f"{book_id}_body.txt"
        header_path.write_text(header, encoding="utf-8", errors="ignore")
        body_path.write_text(body, encoding="utf-8", errors="ignore")

        return {"book_id": int(book_id), "header_path": str(header_path), "body_path": str(body_path),
                "date": ymd, "hour": hh}

    def iter_books(self):
        if not self.root.exists():
            return
        for day_dir in sorted(self.root.iterdir()):
            if not day_dir.is_dir(): continue
            for hour_dir in sorted(day_dir.iterdir()):
                if not hour_dir.is_dir(): continue
                bodies = {p.name.split("_")[0]: p for p in hour_dir.glob("*_body.txt")}
                for hid in hour_dir.glob("*_header.txt"):
                    book_id = hid.name.split("_")[0]
                    body = bodies.get(book_id)
                    if body:
                        yield int(book_id), str(hid), str(body)
