"""BM25 retrieval over table documentation (dependency-free, deterministic, explainable).

For catalogs with a few thousand tables BM25 over well-written descriptions is competitive
with embeddings, needs no vector store, and makes "why was this table chosen" easy to debug.
Swap in an embedding retriever by implementing ``search(query, k)``.
"""

from __future__ import annotations

import math
import re
from collections import Counter

from catalog_agent.catalog import Catalog, TableDoc

_TOKEN = re.compile(r"[a-z0-9]+")
_STOP = frozenset("a an the of for to in on by and or is are was were what which how many much per with from "
                  "show me give list each by do does did our my".split())


def tokenize(text: str) -> list[str]:
    out = []
    for t in _TOKEN.findall(text.lower()):
        if t in _STOP:
            continue
        if len(t) > 4 and t.endswith("s") and not t.endswith("ss"):
            t = t[:-1]  # cheap plural folding
        out.append(t)
    return out


class BM25Retriever:
    def __init__(self, catalog: Catalog, certified_only: bool = True, k1: float = 1.4, b: float = 0.75):
        self.docs: list[TableDoc] = catalog.certified() if certified_only else list(catalog.tables.values())
        self.k1, self.b = k1, b
        self.tf = [Counter(tokenize(d.search_text())) for d in self.docs]
        self.len = [sum(c.values()) for c in self.tf]
        self.avg = sum(self.len) / max(len(self.len), 1)
        df: Counter[str] = Counter()
        for c in self.tf:
            df.update(c.keys())
        n = len(self.docs)
        self.idf = {t: math.log(1 + (n - f + 0.5) / (f + 0.5)) for t, f in df.items()}

    def search(self, query: str, k: int = 3) -> list[tuple[TableDoc, float]]:
        q = tokenize(query)
        scored = []
        for i, doc in enumerate(self.docs):
            s = 0.0
            for t in q:
                f = self.tf[i].get(t)
                if not f:
                    continue
                s += self.idf[t] * f * (self.k1 + 1) / (f + self.k1 * (1 - self.b + self.b * self.len[i] / self.avg))
            if s > 0:
                scored.append((doc, round(s, 4)))
        return sorted(scored, key=lambda x: -x[1])[:k]
