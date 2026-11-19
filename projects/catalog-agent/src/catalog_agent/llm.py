"""LLM adapters. The agent only needs ``complete(system, messages) -> str``."""

from __future__ import annotations

import json
import os
import urllib.request
from collections.abc import Iterable
from typing import Protocol


class LLM(Protocol):
    def complete(self, system: str, messages: list[dict[str, str]]) -> str: ...


class OpenAICompatibleLLM:
    """Works with any ``/chat/completions`` endpoint: OpenAI, Azure OpenAI, Databricks Model
    Serving, vLLM, Ollama. No SDK dependency."""

    def __init__(self, model: str, base_url: str | None = None, api_key_env: str = "OPENAI_API_KEY",
                 timeout: float = 60.0):
        self.model = model
        self.base_url = (base_url or os.environ.get("LLM_BASE_URL", "https://api.openai.com/v1")).rstrip("/")
        self.api_key_env = api_key_env
        self.timeout = timeout

    def complete(self, system: str, messages: list[dict[str, str]]) -> str:  # pragma: no cover - network
        body = json.dumps({
            "model": self.model, "temperature": 0,
            "messages": [{"role": "system", "content": system}, *messages],
        }).encode()
        req = urllib.request.Request(
            f"{self.base_url}/chat/completions", data=body, method="POST",
            headers={"Content-Type": "application/json",
                     "Authorization": f"Bearer {os.environ.get(self.api_key_env, '')}"},
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            return json.load(resp)["choices"][0]["message"]["content"]


class ScriptedLLM:
    """Deterministic replies for tests and offline demos; records every prompt it receives."""

    def __init__(self, replies: Iterable[str]):
        self.replies = list(replies)
        self.calls: list[tuple[str, list[dict[str, str]]]] = []

    def complete(self, system: str, messages: list[dict[str, str]]) -> str:
        self.calls.append((system, [dict(m) for m in messages]))
        if not self.replies:
            raise RuntimeError("ScriptedLLM ran out of replies")
        return self.replies.pop(0)
