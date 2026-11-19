"""Column classification: value evidence + name evidence -> category and confidence."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field

from piiguard.detectors import DETECTORS, NAME_HINTS

_SENS_RANK = {"restricted": 2, "confidential": 1, "public": 0}


@dataclass(frozen=True)
class ColumnClassification:
    column: str
    category: str | None
    sensitivity: str
    confidence: float
    evidence: str
    embedded: bool = False  # PII found inside free text, not the column's primary content
    match_ratios: dict[str, float] = field(default_factory=dict)

    @property
    def is_pii(self) -> bool:
        return self.category is not None


def classify_column(
    column: str, values: Iterable[object], dominant_threshold: float = 0.6, embedded_threshold: float = 0.02
) -> ColumnClassification:
    sample = [str(v) for v in values if v is not None and str(v).strip()]
    ratios: dict[str, float] = {}
    if sample:
        for det in DETECTORS:
            hits = sum(1 for s in sample if det.match(s))
            if hits:
                ratios[det.category] = hits / len(sample)

    hint = next(((cat, sens) for rx, cat, sens in NAME_HINTS if rx.search(column)), None)
    sens_of = {d.category: d.sensitivity for d in DETECTORS}

    best = max(ratios.items(), key=lambda kv: (kv[1], _SENS_RANK[sens_of[kv[0]]]), default=None)
    if best and best[1] >= dominant_threshold:
        cat, ratio = best
        conf = min(1.0, ratio + (0.2 if hint and hint[0] == cat else 0))
        return ColumnClassification(column, cat, sens_of[cat], round(conf, 3),
                                    f"{ratio:.0%} of {len(sample)} sampled values match {cat}", False, ratios)
    if hint:
        cat, sens = hint
        support = ratios.get(cat, 0.0)
        conf = 0.75 + 0.25 * support if sample else 0.5
        return ColumnClassification(column, cat, sens, round(conf, 3),
                                    f"column name suggests {cat}", False, ratios)
    # Free-text columns (notes, comments, descriptions) leaking PII. Only for genuinely textual
    # columns: a few random 16-digit ids will always pass Luhn by chance (~1 in 10).
    is_free_text = bool(sample) and sum(1 for s in sample if " " in s.strip()) / len(sample) >= 0.5
    leaks = {k: v for k, v in ratios.items() if v >= embedded_threshold} if is_free_text else {}
    if leaks:
        cat = max(leaks, key=lambda k: (_SENS_RANK[sens_of[k]], leaks[k]))
        # each hit already passed structural validation (Luhn/SSA/mod-97/RFC), so even a low ratio is strong
        return ColumnClassification(column, cat, sens_of[cat], round(min(0.95, 0.7 + 2 * leaks[cat]), 3),
                                    f"free text contains {cat} in {leaks[cat]:.0%} of rows", True, ratios)
    return ColumnClassification(column, None, "public", 1.0 - max(ratios.values(), default=0.0), "no PII signals", False, ratios)


def classify_table(rows: Sequence[Mapping[str, object]]) -> list[ColumnClassification]:
    cols: list[str] = []
    for r in rows:
        cols.extend(c for c in r if c not in cols)
    return [classify_column(c, (r.get(c) for r in rows)) for c in cols]


def classify_spark_table(spark, table: str, sample_rows: int = 2000) -> list[ColumnClassification]:  # pragma: no cover
    """Sample a table without a full scan and classify every string-like column."""
    df = spark.table(table)
    string_cols = [f.name for f in df.schema.fields if f.dataType.simpleString() in ("string", "bigint", "int")]
    rows = [r.asDict() for r in df.select(*string_cols).limit(sample_rows * 5).sample(fraction=0.2, seed=7)
            .limit(sample_rows).collect()]
    return classify_table(rows)
