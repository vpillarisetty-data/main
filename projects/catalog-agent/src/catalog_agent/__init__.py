"""Governed text-to-SQL and metadata discovery agent over a curated Gold layer."""

from catalog_agent.agent import AgentResult, CatalogAgent
from catalog_agent.catalog import Catalog, Column, TableDoc, load_catalog
from catalog_agent.guardrails import GuardrailError, SqlGuard
from catalog_agent.retriever import BM25Retriever

__all__ = [
    "AgentResult", "BM25Retriever", "Catalog", "CatalogAgent", "Column", "GuardrailError",
    "SqlGuard", "TableDoc", "load_catalog",
]
