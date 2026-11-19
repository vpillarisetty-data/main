"""Retrieve -> generate -> guard -> execute -> self-correct.

The model never sees PII column names (they are removed from the schema context), never sees
tables outside the retrieved certified set, and every query passes the deterministic guard. When
the guard or the warehouse rejects a query, the error is fed back so the model can repair it
(bounded attempts). If no certified dataset matches the question, the agent says so instead of
guessing.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field

from catalog_agent.catalog import Catalog
from catalog_agent.guardrails import GuardrailError, SqlGuard
from catalog_agent.llm import LLM
from catalog_agent.retriever import BM25Retriever

Executor = Callable[[str, Sequence[str]], tuple[list[str], list[tuple]]]

SYSTEM_PROMPT = """You are a careful analytics engineer writing Databricks SQL for business users.
Rules:
- Use ONLY the tables and columns listed below. Always use fully qualified table names.
- Write exactly one read-only SELECT (CTEs allowed). Never modify data.
- Prefer explicit column lists, clear aliases, and ORDER BY for "top"/"most" questions.
- If the question cannot be answered from these tables, reply exactly: CANNOT_ANSWER: <reason>
Return the SQL in a single ```sql fenced block.

{schema}"""

_FENCE = re.compile(r"```(?:sql)?\s*(.*?)```", re.S | re.I)


@dataclass
class AgentResult:
    question: str
    sql: str | None = None
    columns: list[str] = field(default_factory=list)
    rows: list[tuple] = field(default_factory=list)
    tables: tuple[str, ...] = ()
    attempts: int = 0
    error: str | None = None
    trace: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.error is None and self.sql is not None


def extract_sql(text: str) -> str:
    m = _FENCE.search(text)
    return (m.group(1) if m else text).strip()


class CatalogAgent:
    def __init__(self, catalog: Catalog, llm: LLM, executor: Executor, *, top_k: int = 3,
                 min_score: float = 1.0, max_attempts: int = 3, max_rows: int = 1000):
        self.catalog = catalog
        self.llm = llm
        self.executor = executor
        self.retriever = BM25Retriever(catalog)
        self.guard = SqlGuard(catalog, max_rows=max_rows)
        self.top_k, self.min_score, self.max_attempts = top_k, min_score, max_attempts

    def ask(self, question: str, entitlements: Iterable[str] = ()) -> AgentResult:
        ents = tuple(entitlements)
        res = AgentResult(question)
        hits = [(d, s) for d, s in self.retriever.search(question, self.top_k) if s >= self.min_score]
        res.trace.append("retrieved: " + ", ".join(f"{d.name}({s})" for d, s in hits))
        if not hits:
            res.error = "No certified dataset matches this question. Ask the data team to publish one."
            return res

        allowed = [d.name for d, _ in hits]
        schema = "\n\n".join(d.prompt_block(hide_pii="pii_reader" not in ents) for d, _ in hits)
        system = SYSTEM_PROMPT.format(schema=schema)
        messages = [{"role": "user", "content": question}]

        for attempt in range(1, self.max_attempts + 1):
            res.attempts = attempt
            reply = self.llm.complete(system, messages)
            if reply.strip().startswith("CANNOT_ANSWER"):
                res.error = reply.strip()
                return res
            sql = extract_sql(reply)
            try:
                guarded = self.guard.check(sql, ents, allowed)
                cols, rows = self.executor(guarded.sql, guarded.tables)
            except GuardrailError as e:
                feedback = f"Your query was rejected by policy: {e}. Rewrite it."
            except Exception as e:  # warehouse error -> let the model repair it
                feedback = f"The query failed to execute: {e}. Fix it."
            else:
                res.sql, res.columns, res.rows, res.tables = guarded.sql, cols, rows, guarded.tables
                res.trace.append(f"attempt {attempt}: ok")
                return res
            res.trace.append(f"attempt {attempt}: {feedback}")
            messages += [{"role": "assistant", "content": reply}, {"role": "user", "content": feedback}]

        res.error = f"Could not produce a valid query in {self.max_attempts} attempts"
        return res


def sqlite_executor(conn) -> Executor:
    """Local demo executor: maps ``cat.schema.table`` onto sqlite tables named the same way."""

    def run(sql: str, tables: Sequence[str]) -> tuple[list[str], list[tuple]]:
        for t in sorted(tables, key=len, reverse=True):
            sql = re.sub(rf"(?<![\w\"]){re.escape(t)}(?![\w\"])", f'"{t}"', sql, flags=re.I)
        cur = conn.execute(sql)
        return [d[0] for d in cur.description], cur.fetchall()

    return run


def databricks_executor(server_hostname: str, http_path: str, access_token: str) -> Executor:  # pragma: no cover
    from databricks import sql as dbsql

    def run(sql: str, tables: Sequence[str]):
        with dbsql.connect(server_hostname=server_hostname, http_path=http_path,
                           access_token=access_token) as conn, conn.cursor() as cur:
            cur.execute(sql)
            return [d[0] for d in cur.description], cur.fetchall()

    return run
