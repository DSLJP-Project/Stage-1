import re
from repository import tokenize

class QueryEngine:
    def __init__(self, pos_idx, simple_idx, repo):
        self.pos = pos_idx
        self.simple = simple_idx
        self.repo = repo

    def search(self, q, method='tfidf', topk=10):
        q = q.strip()

        if m := re.match(r'^"(.*)"$', q):
            return [{"doc_id": d, "text": self.repo.get(d)[:300]} for d, _ in self.pos.phrase_search(m[1])]

        if " OR " in q:
            terms = [w for t in q.split(" OR ") for w in tokenize(t)]
            return [{"doc_id": d, "text": self.repo.get(d)[:300]} for d in self.simple.boolean_or(terms)]

        terms = tokenize(q)
        if method == 'boolean':
            docs = self.pos.boolean_and(terms)
            docs = [(d, 0) for d in docs]
        else:
            docs = self.pos.tf_idf_score(terms, topk=topk)

        return [
            {"doc_id": d, "text": self.repo.get(d)[:300]} if method == 'boolean'
            else {"doc_id": d, "score": s, "text": self.repo.get(d)[:300]}
            for d, s in docs
        ]
