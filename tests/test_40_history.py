"""Tier 4 — historical search correctness. ~$0.03.

  - A fixed window (from:openai 2026-06-01 .. 2026-07-01) must yield all-unique
    ids, every createdAt inside the window, and two runs must have at least 95%
    id overlap. The upstream search index can omit a small number of ordinary
    mid-window tweets nondeterministically, so exact set equality is not an API
    contract (documented here and in TRIAGE.md).
  - Truncation honesty: a $0.005 ceiling over a 6-month window must exit 0 with
    an unmistakable partial payload while every emitted tweet remains unique
    and inside the window.

Both exercise the sliding-time-window walk that replaces cursor pagination
(cursors infinite-loop on historical ranges — the documented trap).
"""
import json
import unittest
from datetime import datetime, timezone

from tests.e2e_base import E2ETest, require_key

TW_FMT = "%a %b %d %H:%M:%S %z %Y"


def ts_of(day):
    return int(datetime.strptime(day, "%Y-%m-%d")
               .replace(tzinfo=timezone.utc).timestamp())


class TestHistoryWindow(E2ETest):
    WINDOW = ("2026-06-01", "2026-07-01")
    QUERY = "from:openai"

    def _walk(self):
        from twitterapi import Client
        from workflows import history_search
        c = self.track(Client(verbose=False, max_usd=0.05))
        tweets = list(history_search(self.QUERY, *self.WINDOW, client=c,
                                     progress=False))
        return c, tweets

    def test_fixed_window_unique_in_window_and_high_overlap_across_runs(self):
        require_key()
        self.repro(f"python3 workflows.py history openai --since {self.WINDOW[0]} "
                   f"--until {self.WINDOW[1]}  (run twice, compare id sets)")
        since_ts, until_ts = ts_of(self.WINDOW[0]), ts_of(self.WINDOW[1])

        c1, run1 = self._walk()
        ids1 = [t["id"] for t in run1]
        self.log(f"run 1: {len(ids1)} tweets, {c1.requests_made} requests, "
                 f"{c1.spent_credits:,.0f} credits")
        self.assertGreater(len(ids1), 0, "window unexpectedly empty — did the "
                                         "account go quiet or private?")
        self.assertEqual(len(ids1), len(set(ids1)), "ids must be unique")

        bad = []
        for t in run1:
            ts = int(datetime.strptime(t["createdAt"], TW_FMT).timestamp())
            if not (since_ts <= ts < until_ts):
                bad.append((t["id"], t["createdAt"]))
        self.assertEqual(bad, [], f"tweets outside [{self.WINDOW[0]}, "
                                  f"{self.WINDOW[1]}): {bad[:5]}")

        # accounting identity for the walk: every page charged 15cr/tweet-slot,
        # spent must equal 15 x total records returned across pages (>= unique)
        self.assertGreaterEqual(c1.spent_credits, 15 * len(ids1),
                                "spend can't be less than the records kept")
        self.assertEqual(c1.spent_credits % 15, 0,
                         "search spend must be a multiple of the 15cr tweet rate")

        c2, run2 = self._walk()
        ids2 = [t["id"] for t in run2]
        self.log(f"run 2: {len(ids2)} tweets, {c2.requests_made} requests, "
                 f"{c2.spent_credits:,.0f} credits")
        self.assertEqual(len(ids2), len(set(ids2)),
                         "ids must remain unique on the comparison walk")
        overlap = len(set(ids1) & set(ids2)) / max(len(set(ids1) | set(ids2)), 1)
        self.assertGreaterEqual(
            overlap, 0.95,
            f"history walks must substantially agree even though the upstream "
            f"index varies; overlap={overlap:.1%} only_run1="
            f"{sorted(set(ids1) - set(ids2))[:5]} only_run2="
            f"{sorted(set(ids2) - set(ids1))[:5]}")
        self.log(f"sequence order identical across runs: {ids1 == ids2} "
                 f"(id overlap {overlap:.1%}; exact equality is not promised)")


class TestTruncationHonesty(E2ETest):
    def test_ceiling_truncation_exits_0_with_explicit_partial_payload(self):
        require_key()
        p = self.run_cli(["workflows.py", "--max-usd", "0.005", "history",
                          "openai", "--since", "2026-02-01",
                          "--until", "2026-08-01"])
        self.assertEqual(p.returncode, 0,
                         f"valid partial data must exit 0, got {p.returncode}; "
                         f"stderr: {p.stderr[-400:]}")
        self.assertIn("STOPPED", p.stderr)

        lines = [json.loads(line) for line in p.stdout.splitlines() if line.strip()]
        tweets, summary = lines[:-1], lines[-1]
        self.assertFalse(summary["complete"])
        self.assertEqual(summary["completeness"]["status"], "partial")
        self.assertEqual(summary["completeness"]["records_returned"], len(tweets))
        self.assertIn("--max-usd", summary["completeness"]["resume"])
        ids = [t["id"] for t in tweets]
        self.log(f"emitted {len(ids)} tweets before the $0.005 ceiling")
        self.assertGreater(len(ids), 0,
                           "the ceiling allows at least one full page; zero "
                           "emissions means the walk broke, not the ceiling")
        self.assertEqual(len(ids), len(set(ids)), "partial output must still dedupe")
        since_ts, until_ts = ts_of("2026-02-01"), ts_of("2026-08-01")
        bad = []
        for t in tweets:
            ts = int(datetime.strptime(t["created_at"], TW_FMT).timestamp())
            if not (since_ts <= ts < until_ts):
                bad.append((t["id"], t["created_at"]))
        self.assertEqual(bad, [], f"partial output must stay inside the window: {bad[:5]}")

        spend = self.parse_cli_spend(p.stderr)
        if spend:
            requests, credits = spend
            self.log(f"subprocess spend: {requests} requests, {credits} credits")
            # A hard ceiling must never be CROSSED. The walk refuses the page
            # that would exceed it, so a truncated run stops at or under the
            # limit and signals partial data in the JSON summary. Asserting overspend
            # here would lock in the weaker "spend past it, then notice"
            # behaviour this guard exists to prevent.
            self.assertLessEqual(credits / 100_000, 0.005,
                                 "a truncated run must stop at or under the "
                                 "ceiling, never above it")
            self.charge_external(credits=credits, requests=requests)
        else:
            self.charge_external(credits=600, requests=3, derived=True)


if __name__ == "__main__":
    unittest.main()
