"""The one offline transport fake.

ScriptedClient is the real Client — accounting, ceilings, pricing, cache, and
every envelope check above the transport — with only _raw/_raw_json scripted.
Each scripted request books its worst-case cost under the real budget check
and settles it the way Client._send does, so a test sees the same ceiling
behaviour as production, including concurrent in-flight bookings.

A script is either a list of responses, consumed in order (an exception
instance is raised instead of returned; an exhausted list fails the test), or
a callable `(path, params_or_body) -> response`.
"""
from __future__ import annotations

import twitterapi

from tests.e2e_base import SCRIPTS  # noqa: F401  (puts scripts/ on sys.path)


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
        def script(s):
            return s if callable(s) or isinstance(s, list) else list(s)

        self.raw_script, self.json_script = script(raw), script(raw_json)
        self.raw_calls, self.json_calls = [], []

    def _send_scripted(self, method, path, max_credits, script, payload, calls):
        """Client._send with the transport scripted: book the worst case
        before the request, release it to the floor when the server answers
        with an error, and leave it for _charge/_settle on success.
        `/oapi/my/info` is the one free endpoint."""
        free = path == "/oapi/my/info"
        worst = 0.0 if free else (max_credits or twitterapi.MIN_REQUEST_CREDITS)
        self._book_attempt(path, worst, f"{method} {path} request")
        calls.append((method, path, payload))
        try:
            body = _answer(script, path, payload)
        except twitterapi.APIError:
            self._release_to_floor(worst)
            raise
        if not free:
            self._provisional.credits = worst
            self._record_run()
        return body

    def _raw(self, method, path, params=None, *, max_credits=None):
        params = dict(params or {})
        return self._send_scripted(method, path, max_credits, self.raw_script,
                                   params, self.raw_calls)

    def _raw_json(self, method, path, body, *, max_credits=None):
        return self._send_scripted(method, path, max_credits, self.json_script,
                                   body, self.json_calls)


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
