import requests

class API:
    URL_CANDIDATES = [
        "https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt",
        "https://www.gutenberg.org/files/{id}/{id}-0.txt",
        "https://www.gutenberg.org/files/{id}/{id}.txt",
    ]

    def __init__(self, timeout=10):
        self.timeout = timeout
        self.headers = {"User-Agent": "D-SearchEngine/1.0 (student project)"}

    def fetch_gutenberg_text(self, book_id: int) -> str | None:
        last_err = None
        for tpl in self.URL_CANDIDATES:
            url = tpl.format(id=book_id)
            try:
                r = requests.get(url, headers=self.headers, timeout=self.timeout, allow_redirects=True)
                ctype = (r.headers.get("Content-Type") or "").lower()
                if r.status_code == 200 and "html" not in ctype and r.text.strip():
                    return r.text
            except requests.RequestException as e:
                last_err = e
                continue
        print(f"[SKIP] {book_id}: no encontrado como texto plano. Último error: {last_err}")
        return None
