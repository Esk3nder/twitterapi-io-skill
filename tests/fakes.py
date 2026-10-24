"""The one offline transport fake.

ScriptedClient is the real Client — accounting, ceilings, pricing, cache, and
every envelope check above the transport — with only _raw/_raw_json scripted.
Each scripted request passes the real budget check and books the 15-credit
floor, as Client._send would, so a test cannot spend past a ceiling that
production would refuse.

A script is either a list of responses, consumed in order (an exception
instance is raised instead of returned; an exhausted list fails the test), or
a callable `(path, params_or_body) -> response`.
"""
from __future__ import annotations

from tests.e2e_base import SCRIPTS  # noqa: F401  (puts scripts/ on sys.path)

import twitterapi


def _answer(script, path, payload):
    if callable(script):
        return script(path, payload)
    if not script:
        raise AssertionError(f"scripted transport received an extra paid request to {path}")
    value = script.pop(0)
    if isinstance(value, BaseException):
        raise value
    return value


class ScriptedClient(twitterapi.Client):
    """A real Client with a scripted transport. `max_usd` defaults to 1.0 so
    a test never runs without a ceiling by accident."""

    def __init__(self, *, raw=(), raw_json=(), **kwargs):
        kwargs.setdefault("api_key", "test")
        kwargs.setdefault("verbose", False)
        kwargs.setdefault("max_usd", 1.0)
        super().__init__(**kwargs)
        # A list is shared, not copied: instances built from one client_class
        # consume a single queue, as successive real requests would.
        script = lambda s: s if callable(s) or isinstance(s, list) else list(s)
        self.raw_script, self.json_script = script(raw), script(raw_json)
        self.raw_calls, self.json_calls = [], []

    def _book(self, method, path, max_credits):
        """What Client._send does before transport: refuse a request the
        ceiling forbids, then book the 15-credit floor. `/oapi/my/info` is
        the one free endpoint."""
        free = path == "/oapi/my/info"
        if not free:
            self._require_budget(max_credits or twitterapi.MIN_REQUEST_CREDITS,
                                 f"{method} {path}")
        self._book_attempt(path, 0.0 if free else twitterapi.MIN_REQUEST_CREDITS,
                           f"{method} {path} request")

    def _raw(self, method, path, params=None, *, max_credits=None):
        params = dict(params or {})
        self._book(method, path, max_credits)
        self.raw_calls.append((method, path, params))
        return _answer(self.raw_script, path, params)

    def _raw_json(self, method, path, body, *, max_credits=None):
        self._book(method, path, max_credits)
        self.json_calls.append((method, path, body))
        return _answer(self.json_script, path, body)


def client_class(raw=(), raw_json=(), created=None):
    """A ScriptedClient class to patch in where code under test builds its own
    Client. The code's own arguments pass through unchanged (no test ceiling
    is added). Every instance is appended to `created` when given."""
    class Bound(ScriptedClient):
        def __init__(self, *args, **kwargs):
            kwargs.setdefault("max_usd", None)
            super().__init__(*args, raw=raw, raw_json=raw_json, **kwargs)
            if created is not None:
                created.append(self)
    return Bound
