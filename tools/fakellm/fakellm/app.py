"""Application factory wiring the Bedrock, OpenAI and admin routes around one `Engine`."""

from __future__ import annotations

from aiohttp import web

from fakellm import admin, bedrock, openai_api
from fakellm.behaviour import Engine
from fakellm.scenario import Scenario
from fakellm.state import ENGINE

# Embedding batches (Cohere 96 texts, OpenAI chunks) easily exceed aiohttp's 1 MiB default.
_MAX_BODY_BYTES = 32 * 1024 * 1024


def create_app(
    scenario: Scenario | None = None, *, engine: Engine | None = None
) -> web.Application:
    """Build the fakellm aiohttp app from a scenario (or a prepared engine for tests)."""
    app = web.Application(client_max_size=_MAX_BODY_BYTES)
    app[ENGINE] = engine or Engine(scenario or Scenario())
    bedrock.add_routes(app)
    openai_api.add_routes(app)
    admin.add_routes(app)
    return app
