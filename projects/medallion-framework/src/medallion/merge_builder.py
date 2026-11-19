"""Generates Delta Lake MERGE statements for the Silver and Gold layers.

Solves the problems that break naive upserts in financial pipelines:

* **Out-of-order / late-arriving events** - updates only apply when the incoming
  ``sequence_by`` value is newer than what is stored, so a late event can never
  overwrite a newer one.
* **Reversals / deletes** - a ``delete_condition`` (e.g. ``op = 'D'``) becomes a
  sequence-guarded **tombstone**, so a late or replayed older insert cannot resurrect
  the row, and a delete that overtakes its own insert still wins.
* **Duplicate keys inside one micro-batch** - MERGE fails if several source rows
  match the same target row, so the source is reduced to the latest row per key
  first (``dedup_source_sql``).
* **SCD Type 2 history** - classic "staged updates" pattern: one MERGE both
  closes the current version and inserts the new one, atomically.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


def _q(col: str) -> str:
    return col if col.startswith("`") else f"`{col}`"


@dataclass(frozen=True)
class MergeBuilder:
    target: str
    keys: Sequence[str]
    sequence_by: str
    columns: Sequence[str]

    def __post_init__(self) -> None:
        if not self.keys:
            raise ValueError("MERGE requires at least one key column")
        missing = [c for c in (*self.keys, self.sequence_by) if c not in self.columns]
        if missing:
            raise ValueError(f"key/sequence columns not in column list: {missing}")

    # ---------- helpers ----------
    def _on(self, t: str = "t", s: str = "s") -> str:
        return " AND ".join(f"{t}.{_q(k)} <=> {s}.{_q(k)}" for k in self.keys)

    def _newer(self) -> str:
        return f"s.{_q(self.sequence_by)} > t.{_q(self.sequence_by)}"

    def dedup_source_sql(self, source_view: str, delete_condition: str | None = None) -> str:
        """Keep only the latest record per key within the incoming batch.

        The delete condition is evaluated here, against the source only, and exposed as
        ``__is_delete``: inside the MERGE an unqualified ``op = 'D'`` would be ambiguous.
        """
        part = ", ".join(_q(k) for k in self.keys)
        cols = ", ".join(_q(c) for c in self.columns)  # explicit list: portable beyond Databricks SQL
        if delete_condition:
            cols += f", coalesce(({delete_condition}), false) AS `__is_delete`"
        return (
            f"SELECT {cols} FROM (\n"
            f"  SELECT *, row_number() OVER (PARTITION BY {part} ORDER BY {_q(self.sequence_by)} DESC) AS _rn\n"
            f"  FROM {source_view}\n"
            f") WHERE _rn = 1"
        )

    # ---------- SCD 1 ----------
    def scd1(self, source_view: str, delete_condition: str | None = None, delete_mode: str = "soft") -> str:
        """Upsert latest state per key.

        ``delete_mode="soft"`` (default) keeps a **tombstone** row (``__deleted = true``) carrying the
        delete's sequence number. A hard DELETE forgets the key entirely, so an older insert arriving
        later (late data, a replay, or a delete that overtook its insert) would resurrect the row.
        Tombstones block that; purge them with :meth:`purge_tombstones_sql` after the lateness window.
        Consumers read through :meth:`current_view_sql`.
        """
        if delete_mode not in ("soft", "hard"):
            raise ValueError("delete_mode must be 'soft' or 'hard'")
        cols = ", ".join(_q(c) for c in self.columns)
        vals = ", ".join(f"s.{_q(c)}" for c in self.columns)
        set_clause = ",\n    ".join(f"t.{_q(c)} = s.{_q(c)}" for c in self.columns)
        lines = [
            f"MERGE INTO {self.target} AS t",
            f"USING ({self.dedup_source_sql(source_view, delete_condition)}) AS s",
            f"ON {self._on()}",
        ]
        if delete_condition and delete_mode == "soft":
            lines.append(
                f"WHEN MATCHED AND {self._newer()} THEN UPDATE SET\n    {set_clause},\n"
                f"    t.`__deleted` = s.`__is_delete`, t.`__updated_at` = current_timestamp()"
            )
            lines.append(
                f"WHEN NOT MATCHED THEN INSERT ({cols}, `__deleted`, `__updated_at`) "
                f"VALUES ({vals}, s.`__is_delete`, current_timestamp())"
            )
            return "\n".join(lines)
        if delete_condition:
            lines.append(f"WHEN MATCHED AND s.`__is_delete` AND {self._newer()} THEN DELETE")
        lines.append(f"WHEN MATCHED AND {self._newer()} THEN UPDATE SET\n    {set_clause}")
        not_deleted = " AND NOT s.`__is_delete`" if delete_condition else ""
        lines.append(f"WHEN NOT MATCHED{not_deleted} THEN INSERT ({cols}) VALUES ({vals})")
        return "\n".join(lines)

    def current_view_sql(self, view: str) -> str:
        """Consumer-facing view that hides tombstones."""
        cols = ", ".join(_q(c) for c in self.columns)
        return f"CREATE OR REPLACE VIEW {view} AS SELECT {cols} FROM {self.target} WHERE NOT `__deleted`"

    def purge_tombstones_sql(self, retention_days: int) -> str:
        """Run on a schedule; ``retention_days`` must exceed the maximum expected event lateness."""
        return (
            f"DELETE FROM {self.target} WHERE `__deleted` "
            f"AND `__updated_at` < current_timestamp() - INTERVAL {int(retention_days)} DAYS"
        )

    # ---------- SCD 2 ----------
    def scd2(self, source_view: str, track_columns: Sequence[str]) -> str:
        """SCD2 with ``__start_at``, ``__end_at`` and ``__is_current`` columns."""
        if not track_columns:
            raise ValueError("SCD2 requires track_columns")
        changed = " OR ".join(f"NOT (t.{_q(c)} <=> s.{_q(c)})" for c in track_columns)
        key_null = ", ".join(f"NULL AS {_q('__mk_' + k)}" for k in self.keys)
        key_real = ", ".join(f"u.{_q(k)} AS {_q('__mk_' + k)}" for k in self.keys)
        on = " AND ".join(f"t.{_q(k)} <=> s.{_q('__mk_' + k)}" for k in self.keys)
        dedup = self.dedup_source_sql(source_view)
        join_on = " AND ".join(f"t.{_q(k)} <=> u.{_q(k)}" for k in self.keys)
        changed_u = changed.replace("s.", "u.")
        seq = _q(self.sequence_by)
        cols = ", ".join([*(_q(c) for c in self.columns), "`__start_at`", "`__end_at`", "`__is_current`"])
        vals = ", ".join([*(f"s.{_q(c)}" for c in self.columns), f"s.{seq}", "NULL", "true"])
        return (
            f"MERGE INTO {self.target} AS t\n"
            f"USING (\n"
            f"  -- rows that close an existing current version (matched by key)\n"
            f"  SELECT u.*, {key_real}\n"
            f"  FROM ({dedup}) AS u\n"
            f"  UNION ALL\n"
            f"  -- the same changed rows again with NULL merge keys -> forced INSERT of the new version\n"
            f"  SELECT u.*, {key_null}\n"
            f"  FROM ({dedup}) AS u\n"
            f"  JOIN {self.target} AS t ON {join_on} AND t.`__is_current`\n"
            f"  WHERE ({changed_u}) AND u.{seq} > t.{seq}\n"
            f") AS s\n"
            f"ON {on} AND t.`__is_current`\n"
            f"WHEN MATCHED AND ({changed}) AND s.{seq} > t.{seq} THEN UPDATE SET\n"
            f"    t.`__is_current` = false, t.`__end_at` = s.{seq}\n"
            f"WHEN NOT MATCHED THEN INSERT ({cols}) VALUES ({vals})"
        )
