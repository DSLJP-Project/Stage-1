import re
from pathlib import Path

_PAT = {
    "title":    re.compile(r"^Title:\s*(.+)$", re.IGNORECASE | re.MULTILINE),
    "author":   re.compile(r"^Author:\s*(.+)$", re.IGNORECASE | re.MULTILINE),
    "language": re.compile(r"^Language:\s*(.+)$", re.IGNORECASE | re.MULTILINE),
}

def parse_header_text(text: str) -> dict:
    def get(rx):
        m = rx.search(text or "")
        return (m.group(1).strip() if m else "Unknown")
    return {
        "title": get(_PAT["title"]),
        "author": get(_PAT["author"]),
        "language": get(_PAT["language"]),
    }

def parse_header_file(path: str | Path) -> dict:
    txt = Path(path).read_text(encoding="utf-8", errors="ignore")
    return parse_header_text(txt)
