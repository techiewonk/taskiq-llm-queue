"""Host deny-list checks for provider endpoints (`QueueConfig.denied_hosts`).

Patterns are shell globs on the URL's host name (`*.openai.azure.com`), matched case-blind.
Providers run the check before they create a client, so a denied endpoint is never contacted.
"""

from __future__ import annotations

from collections.abc import Iterable
from fnmatch import fnmatchcase
from urllib.parse import urlsplit

from taskiq_llm_queue.errors import ConfigError


def denied_pattern(url: str, denied_hosts: Iterable[str]) -> str | None:
    """Return the first deny pattern matching `url`'s host, or None when the host is allowed.

    A bare host (`api.example.com`) is accepted as well as a full URL.
    """
    host = (urlsplit(url if "//" in url else f"//{url}").hostname or "").lower()
    for pattern in denied_hosts:
        if host and fnmatchcase(host, pattern.strip().lower()):
            return pattern
    return None


def check_url(url: str | None, denied_hosts: Iterable[str]) -> None:
    """Raise `ConfigError` when `url` points at a denied host; None (SDK default) passes."""
    if url is None:
        return
    pattern = denied_pattern(url, denied_hosts)
    if pattern is not None:
        raise ConfigError(f"endpoint {url!r} matches denied host pattern {pattern!r}")
