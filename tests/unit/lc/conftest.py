from __future__ import annotations

from collections.abc import Iterator

import pytest

from taskiq_llm_queue.langchain import set_default_queue


@pytest.fixture(autouse=True)
def _no_default_queue() -> Iterator[None]:
    """Every test starts and ends without a process-wide default queue."""
    set_default_queue(None)
    yield
    set_default_queue(None)
