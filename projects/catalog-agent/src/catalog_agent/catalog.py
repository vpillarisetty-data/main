"""Catalog metadata: the agent's only source of truth about what exists and what it means."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    description: str = ""
    pii: bool = False


@dataclass(frozen=True)
class TableDoc:
    name: str
    description: str
    columns: tuple[Column, ...]
    owner: str = ""
    certified: bool = False
    synonyms: tuple[str, ...] = ()
    sample_questions: tuple[str, ...] = ()

    def search_text(self) -> str:
        cols = " ".join(f"{c.name.replace('_', ' ')} {c.description}" for c in self.columns)
        return " ".join([self.name.replace(".", " ").replace("_", " "), self.description, cols,
                         " ".join(self.synonyms), " ".join(self.sample_questions)])

    def prompt_block(self, hide_pii: bool = True) -> str:
        lines = [f"TABLE {self.name}  -- {self.description}"]
        for c in self.columns:
            if c.pii and hide_pii:
                continue
            lines.append(f"  {c.name} {c.type}" + (f"  -- {c.description}" if c.description else ""))
        return "\n".join(lines)

    @property
    def pii_columns(self) -> frozenset[str]:
        return frozenset(c.name.lower() for c in self.columns if c.pii)


@dataclass
class Catalog:
    tables: dict[str, TableDoc] = field(default_factory=dict)

    def get(self, name: str) -> TableDoc | None:
        return self.tables.get(name.lower())

    def certified(self) -> list[TableDoc]:
        return [t for t in self.tables.values() if t.certified]


def load_catalog(path: str | Path) -> Catalog:
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    cat = Catalog()
    for t in raw["tables"]:
        doc = TableDoc(
            name=t["name"].lower(), description=t.get("description", ""),
            columns=tuple(Column(c["name"], c["type"], c.get("description", ""), bool(c.get("pii")))
                          for c in t["columns"]),
            owner=t.get("owner", ""), certified=bool(t.get("certified")),
            synonyms=tuple(t.get("synonyms", [])), sample_questions=tuple(t.get("sample_questions", [])),
        )
        cat.tables[doc.name] = doc
    return cat
