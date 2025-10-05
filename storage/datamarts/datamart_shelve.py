import shelve
from .datamart_base import Datamart
from collections import defaultdict

class DatamartShelve(Datamart):
    def __init__(self, path: str = "datamart_shelve.db"):
        self.db = shelve.open(path, writeback=True)
        if "_author_index" not in self.db:
            self.db["_author_index"] = defaultdict(set)
        if "_title_index" not in self.db:
            self.db["_title_index"] = defaultdict(set)

    def upsert_many(self, rows):
        count = 0
        aidx = self.db["_author_index"]; tidx = self.db["_title_index"]
        for r in rows:
            key = str(r["book_id"])
            self.db[key] = r
            aidx[r["author"]].add(key)
            tidx[r["title"]].add(key)
            count += 1
        self.db["_author_index"] = aidx; self.db["_title_index"] = tidx
        self.db.sync()
        return count

    def get_by_author(self, author: str) -> list[dict]:
        keys = list(self.db["_author_index"].get(author, []))
        return [self.db[k] for k in keys]

    def get_by_title(self, title: str) -> list[dict]:
        keys = list(self.db["_title_index"].get(title, []))
        return [self.db[k] for k in keys]

    def close(self) -> None:
        self.db.close()
