import re

STOP_WORDS = {
    'the','is','a','an','and','or','of','to','in','that','it','this','for',
    'on','with','as','by','be','are','was','were','has','have','but','not',
    'these','those','i','you'
}

def tokenize(text):
    return [w for w in re.findall(r"\w+", text.lower()) if w not in STOP_WORDS]

class DocumentRepo:
    def __init__(self):
        self.docs, self.meta = {}, {}

    def add(self, doc_id, text, meta=None):
        self.docs[doc_id], self.meta[doc_id] = text, meta or {}

    def get(self, doc_id):
        return self.docs.get(doc_id)

    def all_docs(self):
        return self.docs.items()
