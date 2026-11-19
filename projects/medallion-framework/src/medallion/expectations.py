"""Declarative data-quality expectations compiled to Spark SQL.

Every expectation is a SQL boolean predicate plus an action:

* ``warn``       - keep the row, count the violation in metrics
* ``drop``       - silently drop violating rows (counted)
* ``quarantine`` - route violating rows to a quarantine table with the reasons attached
* ``fail``       - abort the batch if any row violates (hard contract)

Compiling to SQL (instead of Python UDFs) keeps everything inside Catalyst, so
checks cost one projection, not a Python round-trip per row.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Action(str, Enum):
    WARN = "warn"
    DROP = "drop"
    QUARANTINE = "quarantine"
    FAIL = "fail"


@dataclass(frozen=True)
class Expectation:
    name: str
    expr: str
    action: Action = Action.WARN

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Expectation:
        if not d.get("name") or not d.get("expr"):
            raise ValueError(f"expectation needs 'name' and 'expr': {d}")
        return cls(name=d["name"], expr=d["expr"], action=Action(d.get("action", "warn")))

    def violation_sql(self) -> str:
        # NULL-safe: a predicate evaluating to NULL is a violation, not a pass.
        return f"NOT coalesce(({self.expr}), false)"


@dataclass(frozen=True)
class ExpectationSuite:
    expectations: tuple[Expectation, ...] = field(default_factory=tuple)

    def by_action(self, *actions: Action) -> tuple[Expectation, ...]:
        return tuple(e for e in self.expectations if e.action in actions)

    @property
    def needs_quarantine(self) -> bool:
        return bool(self.by_action(Action.QUARANTINE))

    def reasons_column_sql(self, alias: str = "_dq_violations") -> str:
        """Array of violated expectation names (empty array == clean row)."""
        if not self.expectations:
            return f"CAST(array() AS array<string>) AS {alias}"
        parts = ", ".join(
            f"CASE WHEN {e.violation_sql()} THEN '{e.name}' END" for e in self.expectations
        )
        return f"filter(array({parts}), x -> x IS NOT NULL) AS {alias}"

    def _any_violation(self, actions: tuple[Action, ...]) -> str:
        exps = self.by_action(*actions)
        if not exps:
            return "false"
        return " OR ".join(f"({e.violation_sql()})" for e in exps)

    def keep_predicate(self) -> str:
        """Rows that continue to Silver."""
        return f"NOT ({self._any_violation((Action.DROP, Action.QUARANTINE, Action.FAIL))})"

    def quarantine_predicate(self) -> str:
        return self._any_violation((Action.QUARANTINE,))

    def fail_predicate(self) -> str:
        return self._any_violation((Action.FAIL,))

    def metrics_sql(self) -> list[str]:
        """Aggregate expressions producing a violation count per expectation."""
        return [
            f"sum(CASE WHEN {e.violation_sql()} THEN 1 ELSE 0 END) AS `{e.name}`"
            for e in self.expectations
        ]
