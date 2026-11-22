"""Negative tests for the defects the 2026-11-22 Astra review reproduced
against a2f4897. Each docstring names the review's finding number."""
from __future__ import annotations

import contextlib
import io
import json
import re
import unittest
from unittest import mock

import jobs
import realtime
import workflows
from cohort import Cohort
from twitterapi import Client, CostLimitExceeded, IncompleteDataError, UsageError

from tests.e2e_base import E2ETest
from tests.fakes import ScriptedClient
from tests.test_05_invariants import MemoryStore, tweet
from tests.test_07_client import _live_client, _page, _Response

NOW = 1_800_000_000


class TestCeilingCannotBeSidestepped(E2ETest):
    def test_a_page_size_under_the_server_minimum_is_reserved_at_the_minimum(self):
        """#1: followers page_size=1 still returns, and bills, 20 profiles;
        reserving for one let a USD 0.00015 ceiling pay USD 0.0006."""
        self.assertEqual(Client.effective_page_size("followers", 1), 20)
        c = ScriptedClient(raw=[], max_usd=0.00015)
        with self.assertRaises(CostLimitExceeded):
            list(c.paginate("followers", "alpha", page_size=1))
        self.assertEqual(c.raw_calls, [])

    def test_a_ceiling_that_is_not_a_finite_amount_is_refused(self):
        """#11: NaN compares false against everything, so it disabled the
        ceiling while looking like one."""
        for bad in (float("nan"), float("inf"), -1.0):
            with self.subTest(bad=bad), self.assertRaises(UsageError):
                Client(api_key="test", verbose=False, max_usd=bad)

    def test_an_id_batch_over_the_endpoint_maximum_is_refused_locally(self):
        """#36: the cost was reserved for 100 ids while every id was sent."""
        c = ScriptedClient(raw=[], max_usd=1)
        with self.assertRaises(UsageError):
            list(c.paginate("tweets_by_ids", ",".join(str(i) for i in range(101))))
        self.assertEqual(c.raw_calls, [])

    def test_a_request_waiting_for_a_send_slot_is_not_sent_after_a_halt(self):
        """#10: a halt was checked only before the throttle wait, so shard
        threads already queued there still sent, and paid for, a request."""
        c, sent = _live_client(max_usd=1.0), []

        def urlopen(req, timeout=None):
            sent.append(req.full_url)
            return _Response(_page([]))
        with mock.patch.object(c, "_throttle", lambda: c.halt("stopped by the user")), \
                mock.patch("urllib.request.urlopen", urlopen), \
                self.assertRaises(CostLimitExceeded):
            list(c.paginate("mentions", "alpha"))
        self.assertEqual(sent, [])
        self.assertEqual((c.requests_made, c.spent_credits), (0, 0.0))

    def test_second_splitting_counts_towards_max_pages(self):
        """#6: the two split requests were neither counted nor capped, so
        `--max-pages 2` made four paid requests and reported complete."""
        full = {"tweets": [tweet(i, NOW) for i in range(20)], "has_next_page": True}
        half = {"tweets": [tweet("r", NOW)], "has_next_page": False}
        c = ScriptedClient(
            raw=lambda path, p: dict(half if "filter:replies" in p["query"] else full),
            store=MemoryStore())
        with self.assertRaises(IncompleteDataError) as stop:
            list(workflows.search_window(c, "from:alpha", NOW - 5, NOW + 5, max_pages=2))
        self.assertEqual(len(c.raw_calls), 2)
        self.assertIn("--max-pages 2", str(stop.exception))


class TestLedger(E2ETest):
    def test_two_clients_started_in_the_same_second_keep_separate_rows(self):
        """#8: the run id was seconds plus pid, so the second client's spend
        overwrote the first's."""
        with mock.patch("twitterapi.time.time", return_value=NOW):
            a, b = _live_client(1.0), _live_client(1.0)
        self.assertNotEqual(a.run_id, b.run_id)

    def test_failed_requests_reach_the_ledger(self):
        """#7: only successes were recorded, so a run of transport failures
        booked credits and left no row."""
        import http.client
        store = self.fresh_store("ledger-failures")
        c = Client(api_key="test", verbose=False, max_usd=1.0, qps=1000, store=store)

        def urlopen(req, timeout=None):
            raise http.client.RemoteDisconnected("closed")
        with mock.patch("urllib.request.urlopen", urlopen), \
                mock.patch("twitterapi.time.sleep"), \
                self.assertRaises(http.client.RemoteDisconnected):
            list(c.paginate("mentions", "alpha"))
        runs = store.stats()["recent_runs"]
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["requests"], 5)
        self.assertGreater(runs[0]["usd"], 0)


class TestCacheGenerations(E2ETest):
    def test_a_refreshed_first_page_never_continues_into_an_older_crawl(self):
        """#14: with --max-age, an expired first page was bought again and
        then joined to the old crawl's still-fresh continuation page."""
        store = self.fresh_store("crawl-generations")
        old = [{"ids": ["old-1"], "has_next_page": True, "next_cursor": "c"},
               {"ids": ["old-2"], "has_next_page": False, "next_cursor": ""}]
        first = ScriptedClient(raw=list(old), store=store)
        self.assertEqual(list(first.paginate("follower_ids", "alpha")), ["old-1", "old-2"])
        store.db.execute("UPDATE pages SET fetched_at = fetched_at - 1000 "
                         "WHERE params NOT LIKE '%\"cursor\": \"c\"%'")
        store.db.commit()

        new = [{"ids": ["new-1"], "has_next_page": True, "next_cursor": "c"},
               {"ids": ["new-2"], "has_next_page": False, "next_cursor": ""}]
        rerun = ScriptedClient(raw=list(new), store=store, cache_max_age=500)
        self.assertEqual(list(rerun.paginate("follower_ids", "alpha")), ["new-1", "new-2"])
        self.assertEqual(len(rerun.raw_calls), 2)

    def test_a_cohort_from_an_incomplete_id_list_is_incomplete(self):
        """#17: follower_ids_for labels an unfinished crawl; from_ids dropped
        the label and the cohort read as the whole audience."""
        class Ids(list):
            complete = False
        store = self.fresh_store("cohort-incomplete-ids")
        co = Cohort.from_ids(Ids(["1", "2"]), store=store)
        self.assertFalse(co.complete)
        self.assertTrue(co.incomplete_reasons)
        self.assertTrue(Cohort.from_ids(["1"], store=store).complete)


class TestOutputsSayWhatTheyMean(unittest.TestCase):
    def test_rising_terms_are_only_terms_that_rose(self):
        """#25: a term that fell from two mentions to one was listed as
        rising."""
        def hashtags(tweet_id, ts, *tags):
            return {**tweet(tweet_id, ts), "text": " ".join(f"#{t}" for t in tags),
                    "entities": {"hashtags": [{"text": t} for t in tags]}}
        day = 86400
        rows = [hashtags("a", NOW - 10, "up", "up2", "down"),
                hashtags("b", NOW - 20, "up"),
                hashtags("c", NOW - day - 10, "down", "up"),
                hashtags("d", NOW - day - 20, "down")]
        with mock.patch.object(jobs, "search_shards", lambda *a, **k: iter(rows)):
            out = jobs.narrative_tracker("topic", days=1, now=NOW,
                                         client=mock.Mock(spend_report=lambda: ""))
        rising = {r["term"].lower().lstrip("#") for r in out["rising_hashtags"]}
        self.assertIn("up", rising)
        self.assertNotIn("down", rising)

    def test_a_stream_event_keeps_the_fields_the_caller_asked_for(self):
        """#26: the event was cut to five fields first, so --fields likes
        printed null for a count the event carried."""
        event = {"event_type": "tweet", "rule_tag": "r1",
                 "tweets": [{"id": "9", "text": "hi", "likeCount": 7,
                             "author": {"userName": "alpha"}}]}
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            realtime._print_event(event, fields="id,likes,rule")
        self.assertEqual(json.loads(out.getvalue()), {"id": "9", "likes": 7, "rule": "r1"})

    def test_every_monitor_query_fits_the_length_limit_as_sent(self):
        """#23: the budget ignored the parentheses and time bounds, so the
        queries actually sent were 525 characters against a 480 limit."""
        handles = [f"account_{i:06d}" for i in range(60)]
        client = ScriptedClient(raw=lambda path, p: {
            "tweets": [], "has_next_page": False}, max_usd=1)
        with mock.patch.object(workflows.time, "time", return_value=NOW), \
                contextlib.redirect_stderr(io.StringIO()):
            workflows.monitor(handles, client=client, once=True, on_tweet=lambda t: None)
        queries = [p["query"] for _, path, p in client.raw_calls
                   if path.endswith("advanced_search")]
        self.assertEqual([h for q in queries for h in re.findall(r"from:(\w+)", q)], handles)
        self.assertLessEqual(max(map(len, queries)), workflows.MAX_QUERY_CHARS)


if __name__ == "__main__":
    unittest.main()
