import re
from collections import defaultdict, Counter
import math

STOP_WORDS = {"the", "a", "an", "is", "are", "of", "and", "to", "in", "on", "for", "with", "that"}

class SimpleInvertedIndex:
    
    def __init__(self):
        self.index = defaultdict(Counter)

    def index_document(self, doc_id, text):
        words = re.findall(r"\w+", text.lower())
        for word in words:
            if word not in STOP_WORDS:
                self.index[word][doc_id] += 1

    def boolean_or(self, terms):
        result = set()
        for term in terms:
            if term in self.index:
                result.update(self.index[term].keys())
        return result

    def boolean_and(self, terms):
        sets = [set(self.index[t].keys()) for t in terms if t in self.index]
        if not sets:
            return set()
        return set.intersection(*sets)

    def tf_idf_score(self, terms, topk=10):
        doc_scores = Counter()
        N = len({doc for docs in self.index.values() for doc in docs})
        for term in terms:
            if term not in self.index:
                continue
            df = len(self.index[term])
            idf = math.log((N + 1) / (df + 1)) + 1
            for doc_id, tf in self.index[term].items():
                doc_scores[doc_id] += tf * idf
        return doc_scores.most_common(topk)

    def stats(self):
        total_terms = sum(sum(docs.values()) for docs in self.index.values())
        unique_docs = {doc for docs in self.index.values() for doc in docs}
        return {
            "docs_indexed": len(unique_docs),
            "unique_terms": len(self.index),
            "total_occurrences": total_terms
        }

    def debug_print(self, limit=10):
        print(f"\n=== DEBUG: Primeras {limit} palabras en el índice simple ===")
        for i, (word, docs) in enumerate(self.index.items()):
            if i >= limit:
                break
            print(f"{word} -> {dict(docs)}")


class PositionalInvertedIndex:
    
    def __init__(self):
        self.index = defaultdict(lambda: defaultdict(list))

    def index_document(self, doc_id, text):
        words = re.findall(r"\w+", text.lower())
        for pos, word in enumerate(words):
            if word not in STOP_WORDS:
                self.index[word][doc_id].append(pos)

    def boolean_or(self, terms):
        result = set()
        for term in terms:
            if term in self.index:
                result.update(self.index[term].keys())
        return result

    def boolean_and(self, terms):
        sets = [set(self.index[t].keys()) for t in terms if t in self.index]
        if not sets:
            return set()
        return set.intersection(*sets)

    def tf_idf_score(self, terms, topk=10):
        doc_scores = Counter()
        N = len({doc for docs in self.index.values() for doc in docs})
        for term in terms:
            if term not in self.index:
                continue
            df = len(self.index[term])
            idf = math.log((N + 1) / (df + 1)) + 1
            for doc_id, positions in self.index[term].items():
                tf = len(positions)
                doc_scores[doc_id] += tf * idf
        return doc_scores.most_common(topk)

    def phrase_search(self, phrase):
        words = re.findall(r"\w+", phrase.lower())
        if not words:
            return []
        if any(word not in self.index for word in words):
            return []

        first_word_docs = self.index[words[0]]
        results = Counter()
        for doc_id, first_positions in first_word_docs.items():
            for pos in first_positions:
                if all((pos + i) in self.index[words[i]].get(doc_id, []) for i in range(1, len(words))):
                    results[doc_id] += 1
        return results.most_common()

    def stats(self):
        total_positions = sum(len(pos_list) for docs in self.index.values() for pos_list in docs.values())
        unique_docs = {doc for docs in self.index.values() for doc in docs}
        return {
            "docs_indexed": len(unique_docs),
            "unique_terms": len(self.index),
            "total_occurrences": total_positions
        }

    def debug_print(self, limit=5):
        print(f"\n=== DEBUG: Primeras {limit} palabras en el índice posicional ===")
        for i, (word, docs) in enumerate(self.index.items()):
            if i >= limit:
                break
            print(f"{word} -> {{ {', '.join(f'{doc}: {positions[:5]}' for doc, positions in docs.items())} }}")
