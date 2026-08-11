"""Shared harness for the twitterapi.io E2E suite. Stdlib only.

Design rules this file enforces:
  - NO CORE-FLOW MOCKS. Tests use the real Client, real Store, real HTTP.
    The only instrumentation permitted is *observation*: record_charges()
    wraps client._charge so a test can assert the accounting identity, but
    every wrapped call still delegates to the real method.
  - STORE ISOLATION. Every Store lives under E2E_STORE_DIR (a temp dir).
    ~/.twitterapi-cache is never touched; a guard below hard-fails if the
    resolved store dir would land inside it. Subprocesses get HOME overridden
    so their default Store() also lands in the temp dir.
  - COST ACCOUNTING. Every test appends a JSONL record (requests, credits,
    usd, seconds, status) to E2E_COST_LOG; run_e2e.sh aggregates it and
    hard-fails the run past the budget. Costs come from client-side
    accounting (spent_credits/page_credits) — never from balance reads,
    which settle 20-60s late server-side.
  - DIAGNOSTICS. self.log() lines go to stderr as the test runs, so a failed
    assertion is diagnosable from the log alone: request counts, credits,
    response key sets, store stats and an exact reproduction command.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import unittest

SKILL = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS = os.path.join(SKILL, "scripts")
if SCRIPTS not in sys.path:
    sys.path.insert(0, SCRIPTS)

HAVE_KEY = bool(os.environ.get("TWITTERAPI_IO_KEY"))

# -- store isolation ---------------------------------------------------------
STORE_DIR = os.environ.get("E2E_STORE_DIR") or tempfile.mkdtemp(prefix="twapi-e2e-")
os.makedirs(STORE_DIR, exist_ok=True)
_PROD_CACHE = os.path.realpath(os.path.expanduser("~/.twitterapi-cache"))
if os.path.realpath(STORE_DIR).startswith(_PROD_CACHE):
    raise RuntimeError(
        f"E2E_STORE_DIR={STORE_DIR} resolves inside the production cache "
        f"{_PROD_CACHE}; refusing to run. Point E2E_STORE_DIR at a temp dir.")

COST_LOG = os.environ.get("E2E_COST_LOG")


def require_key():
    """Missing/invalid key is a SKIP with notice, not a failure (TRIAGE.md)."""
    if not HAVE_KEY:
        raise unittest.SkipTest(
            "TWITTERAPI_IO_KEY not set — live E2E test skipped (not a "
            "failure). Export the key (see ~/.zshenv) or set the CI secret.")


def record_charges(client):
    """Observe (not mock) every _charge call on a real client.

    Returns a list that fills with (endpoint_name, records_in_page, page_size)
    tuples as real pages are fetched and billed. Used to assert the accounting
    identity: spent_credits == sum(page_credits(*event) for events)."""
    events = []
    orig = client._charge

    def wrapper(name, records_in_page, page_size):
        events.append((name, records_in_page, page_size))
        return orig(name, records_in_page, page_size)

    client._charge = wrapper
    return events


class E2ETest(unittest.TestCase):
    """Base class: client tracking, cost accounting, diagnostics, repro."""
    maxDiff = None

    def setUp(self):
        self._t0 = time.monotonic()
        self._clients = []
        self._external_credits = 0.0     # subprocess spend, parsed or derived
        self._external_requests = 0
        self._external_derived = False

    # -- tracking ----------------------------------------------------------
    def track(self, client):
        self._clients.append(client)
        return client

    def charge_external(self, credits, requests, derived=False):
        """Account spend by a subprocess (parsed from its spend report where
        printed, otherwise derived from the measured price table)."""
        self._external_credits += credits
        self._external_requests += requests
        self._external_derived = self._external_derived or derived

    # -- diagnostics ---------------------------------------------------------
    def log(self, msg):
        print(f"    [{self._name()}] {msg}", file=sys.stderr, flush=True)

    def _name(self):
        return self.id().split(".")[-1]

    def repro(self, cmd):
        self.log(f"repro: {cmd}")

    def dump_resp(self, label, resp):
        if isinstance(resp, dict):
            self.log(f"{label}: top-level keys={sorted(resp.keys())} "
                     f"body[:300]={json.dumps(resp, ensure_ascii=False)[:300]}")
        else:
            self.log(f"{label}: type={type(resp).__name__} value[:300]={str(resp)[:300]}")

    def dump_store(self, store):
        try:
            self.log(f"store stats: {json.dumps(store.stats())}")
        except Exception as e:
            self.log(f"store stats unavailable: {e}")

    # -- store helpers -------------------------------------------------------
    def store_path(self, name):
        return os.path.join(STORE_DIR, f"{name}.db")

    def fresh_store(self, name):
        from store import Store
        return Store(self.store_path(name))

    # -- subprocess helper ---------------------------------------------------
    def run_cli(self, args, timeout=300):
        """Run a scripts/ CLI with HOME pointed at the isolated store dir, so
        any default Store() a script constructs cannot touch the real
        ~/.twitterapi-cache. Returns CompletedProcess; logs everything."""
        env = dict(os.environ)
        env["HOME"] = STORE_DIR
        cmd = [sys.executable] + args
        self.repro(f"HOME={STORE_DIR} {' '.join(cmd)}")
        p = subprocess.run(cmd, cwd=SCRIPTS, env=env, capture_output=True,
                           text=True, timeout=timeout)
        self.log(f"exit={p.returncode} stdout={len(p.stdout)}B stderr:")
        for line in p.stderr.strip().splitlines()[-12:]:
            self.log(f"  stderr| {line}")
        return p

    def parse_cli_spend(self, stderr_text):
        """Extract (requests, credits) from a spend_report line like
        '3 requests, ~$0.0002 (18 credits) this session'. None if absent."""
        import re
        m = re.search(r"([\d,]+) requests, ~\$[\d.,]+ \(([\d,]+) credits\)",
                      stderr_text)
        if not m:
            return None
        return (int(m.group(1).replace(",", "")),
                float(m.group(2).replace(",", "")))

    # -- cost report ---------------------------------------------------------
    def _status(self):
        out = getattr(self, "_outcome", None)
        res = getattr(out, "result", None) if out else None
        if res is None:
            return "unknown"
        bad = [t for t, _ in list(getattr(res, "failures", []))
               + list(getattr(res, "errors", []))]
        return "FAIL" if self in bad else "pass"

    def tearDown(self):
        elapsed = time.monotonic() - self._t0
        req = sum(c.requests_made for c in self._clients) + self._external_requests
        credits = sum(c.spent_credits for c in self._clients) + self._external_credits
        saved = sum(c.saved_credits for c in self._clients)
        hits = sum(c.cache_hits for c in self._clients)
        usd = credits / 100_000
        status = self._status()
        self.log(f"DONE status={status} requests={req} credits={credits:,.1f} "
                 f"usd=${usd:.5f} cache_hits={hits} saved_credits={saved:,.1f} "
                 f"elapsed={elapsed:.1f}s"
                 + (" (subprocess cost partly derived)" if self._external_derived else ""))
        if COST_LOG:
            with open(COST_LOG, "a") as fh:
                fh.write(json.dumps({
                    "test": self.id().split("tests.")[-1],
                    "status": status, "requests": req,
                    "credits": round(credits, 2), "usd": round(usd, 6),
                    "saved_credits": round(saved, 2), "cache_hits": hits,
                    "seconds": round(elapsed, 1),
                    "derived": self._external_derived}) + "\n")
