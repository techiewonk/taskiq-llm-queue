"""Typed aiohttp app key under which every handler finds the shared `Engine`."""

from __future__ import annotations

from aiohttp import web

from fakellm.behaviour import Engine

ENGINE = web.AppKey("fakellm_engine", Engine)
